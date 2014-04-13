package inotify

import (
	"sync"
)

type Event struct {
	Mask   uint32
	Cookie uint32
	Name   string
}

func (e *Event) Match(event uint32) bool {
	return true
}

type watch struct {
	wd    uint32
	flags uint32
}

type Watcher struct {
	mu       sync.Mutex
	fd       int               // File descriptor (as returned by the inotify_init() syscall)
	watches  map[string]*watch // Map of inotify watches (key: path)
	paths    map[int]string    // Map of watched paths (key: watch descriptor)
	Error    chan error        // Errors are sent on this channel
	Event    chan *Event       // Events are returned on this channel
	done     chan bool         // Channel for sending a "quit message" to the reader goroutine
	isClosed bool              // Set to true when Close() is first called
}

func NewWatcher() (*Watcher, error) {
	return new(Watcher), nil
}

func (w *Watcher) Close() error {
	return nil
}

func (w *Watcher) AddWatch(path string, flags uint32) error {
	return nil
}

func (w *Watcher) Watch(path string) error {
	return nil
}

func (w *Watcher) RemoveWatch(path string) error {
	return nil
}

// String formats the event e in the form
// "filename: 0xEventMask = IN_ACCESS|IN_ATTRIB_|..."
func (e *Event) String() string {
	return "mock watcher"
}

const (
	IN_DONT_FOLLOW uint32 = iota
	IN_ONESHOT
	IN_ONLYDIR

	IN_ACCESS
	IN_ALL_EVENTS
	IN_ATTRIB
	IN_CLOSE
	IN_CLOSE_NOWRITE
	IN_CLOSE_WRITE
	IN_CREATE
	IN_DELETE
	IN_DELETE_SELF
	IN_MODIFY
	IN_MOVE
	IN_MOVED_FROM
	IN_MOVED_TO
	IN_MOVE_SELF
	IN_OPEN

	// Special events
	IN_ISDIR
	IN_IGNORED
	IN_Q_OVERFLOW
	IN_UNMOUNT
)
