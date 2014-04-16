package main

import (
	"os"
	"log"
	"inotify"
)

func main() {
	listenDir := os.Args[1]
	watcher, err := inotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	err = watcher.Watch(listenDir)
	if err != nil {
		panic(err)
	}
	
	for {
	select {
	case ev := <-watcher.Event:
		switch {
		case ev.Match(inotify.IN_OPEN):
		case ev.Match(inotify.IN_CLOSE):
		case ev.Match(inotify.IN_CLOSE_WRITE):
		case ev.Match(inotify.IN_CLOSE_NOWRITE):
		case ev.Match(inotify.IN_ACCESS):
		case ev.Match(inotify.IN_MODIFY):
		default:
			log.Println("[INFO]", ev)
		}
	case err := <-watcher.Error:
		log.Println("[ERROR]", err)
	}
	}
}
