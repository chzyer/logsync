package main

import (
	"dirwriter"
)

func main() {
	println("started")
	d, err := dirwriter.New(":8303", "./")
	if err != nil {
		panic(err)
	}
	d.Run()
}
