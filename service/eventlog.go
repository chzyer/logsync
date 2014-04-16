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
		log.Println("[INFO]", ev)
	case err := <-watcher.Error:
		log.Println("[ERROR]", err)
	}
	}
}
