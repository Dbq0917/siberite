package service

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/bogdanovich/siberite/controller"
	"github.com/bogdanovich/siberite/repository"
)

// Service represents a siberite tcp server
type Service struct {
	dataDir string
	repo    *repository.QueueRepository
	ch      chan struct{}
	wg      *sync.WaitGroup

	// MaxQueueLength bounds each queue. Zero keeps queues unbounded.
	MaxQueueLength uint64

	// MaxConnections bounds active client sessions. Zero leaves acceptance unlimited.
	MaxConnections int

	// FullRetryAfter optionally overrides the drain-rate estimate for full queues.
	FullRetryAfter time.Duration
}

// New creates a new service
func New(dataDir string) *Service {
	s := &Service{
		dataDir: dataDir,
		repo:    &repository.QueueRepository{},
		ch:      make(chan struct{}),
		wg:      &sync.WaitGroup{},
	}
	s.wg.Add(1)
	return s
}

// Serve starts the service
func (s *Service) Serve(laddr *net.TCPAddr) {
	defer s.wg.Done()

	log.Println("initializing...")
	log.Println("data directory: ", s.dataDir)
	if err := s.initRepository(); err != nil {
		log.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", laddr)
	if nil != err {
		log.Fatalln(err)
	}
	log.Println("listening on", listener.Addr())

	s.serveTCP(listener)
}

func (s *Service) initRepository() error {
	repo, err := repository.NewRepositoryWithOptions(s.dataDir, &repository.Options{
		MaxQueueLength: s.MaxQueueLength,
		FullRetryAfter: s.FullRetryAfter,
	})
	if err != nil {
		return err
	}
	s.repo = repo
	return nil
}

// ServeTCP runs the accept loop on an existing TCP listener.
func (s *Service) ServeTCP(listener *net.TCPListener) {
	defer s.wg.Done()
	s.serveTCP(listener)
}

func (s *Service) serveTCP(listener *net.TCPListener) {
	var connectionSlots chan struct{}
	if s.MaxConnections > 0 {
		connectionSlots = make(chan struct{}, s.MaxConnections)
	}

	for {
		select {
		case <-s.ch:
			log.Println("stopping listening on", listener.Addr())
			listener.Close()
			return
		default:
		}
		listener.SetDeadline(time.Now().Add(1e9))
		conn, err := listener.AcceptTCP()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println(err)
			continue
		}

		if connectionSlots != nil {
			select {
			case connectionSlots <- struct{}{}:
			default:
				conn.Close()
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer func() {
				if connectionSlots != nil {
					<-connectionSlots
				}
			}()
			s.handleConnection(conn)
		}()
	}
}

// Stop service
func (s *Service) Stop() {
	log.Println("stopping service and finishing work...")
	close(s.ch)
	s.wg.Wait()
}

func (s *Service) handleConnection(conn *net.TCPConn) {
	defer conn.Close()
	defer s.wg.Done()

	c := controller.NewSession(conn, s.repo)
	defer c.FinishSession()

	for {
		select {
		case <-s.ch:
			log.Println("disconnecting", conn.RemoteAddr())
			return
		default:
		}
		err := c.Dispatch()
		if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
			continue
		}
		if err != nil {
			if err == controller.ErrClientQuit || err.Error() == "EOF" {
				return
			}
			log.Println(conn.RemoteAddr(), err)
			return
		}
	}
}

// Version returns siberite version
func (s *Service) Version() string {
	return repository.Version
}
