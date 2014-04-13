package main

import (
	"localdir"
)

func main() {
	println("started")
	d, err := localdir.NewDir(":8303", "/disk2/")
	if err != nil {
		panic(err)
	}
	d.Sync()
}
