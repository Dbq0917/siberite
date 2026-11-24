package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	service "github.com/bogdanovich/siberite/service"
)

var (
	dataDir        = flag.String("data", "./data", "path to data directory")
	hostAndPort    = flag.String("listen", "0.0.0.0:22133", "ip and port to listen")
	pidPath        = flag.String("pid", "", "path to PID file to use")
	versionFlag    = flag.Bool("version", false, "prints current version")
	maxQueueItems  = flag.Uint64("max-queue-items", 0, "maximum live items per queue; zero means unlimited")
	maxConnections = flag.Int("max-connections", 0, "maximum concurrent client connections; zero means unlimited")
	fullRetryAfter = flag.Duration("full-retry-after", 0, "fixed retry hint for full queues; zero estimates from drain rate and falls back to 100ms")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalln(err)
	}

	svc := service.New(*dataDir)
	svc.MaxQueueLength = *maxQueueItems
	svc.MaxConnections = *maxConnections
	svc.FullRetryAfter = *fullRetryAfter

	if *versionFlag {
		fmt.Println(svc.Version())
		os.Exit(0)
	}

	// Write a PID file if its requested
	if len(*pidPath) > 0 {
		if err := os.WriteFile(*pidPath, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.Fatalln(err)
		}
		defer os.Remove(*pidPath)
	}

	laddr, err := net.ResolveTCPAddr("tcp", *hostAndPort)
	if err != nil {
		log.Fatalln(err)
	}

	go svc.Serve(laddr)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	svc.Stop()
}
