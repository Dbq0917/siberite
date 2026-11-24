package cgroup

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/bogdanovich/siberite/queue"
)

// make sure CGQueue implements Consumer interface
var _ queue.Consumer = (*CGQueue)(nil)

// CGQueue represents queue with multiple consumer groups
type CGQueue struct {
	sync.Mutex
	Name    string
	dataDir string
	options *queue.Options
	*queue.Queue
	*CGManager
}

// CGQueueOpen opens a queue with multiple consumer groups
func CGQueueOpen(name string, dataDir string) (*CGQueue, error) {
	return CGQueueOpenWithOptions(name, dataDir, &queue.Options{})
}

// CGQueueOpenWithOptions opens a queue with the provided queue options.
func CGQueueOpenWithOptions(name string, dataDir string, options *queue.Options) (*CGQueue, error) {
	if options == nil {
		options = &queue.Options{}
	}
	q := &CGQueue{Name: name, dataDir: filepath.Join(dataDir, name), options: options}
	return q, q.initialize()
}

// Close closes the queue
func (q *CGQueue) Close() {
	q.CGManager.Close()
	q.Queue.Close()
}

// Drop closes the queue and removes it's data directory
func (q *CGQueue) Drop() {
	q.Close()
	os.RemoveAll(q.Path())
}

// Flush drops all queue data
func (q *CGQueue) Flush() error {
	q.Lock()
	defer q.Unlock()
	q.Drop()
	return q.initialize()
}

// Path returns queue data directory path
func (q *CGQueue) Path() string {
	return q.dataDir
}

func (q *CGQueue) initialize() error {
	var err error
	q.Queue, err = queue.Open(q.Name, q.dataDir, q.options)
	if err != nil {
		return err
	}

	q.CGManager, err = NewCGManager(filepath.Join(q.dataDir, "_.metadata"), q.Queue)
	return err
}
