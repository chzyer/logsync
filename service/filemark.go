package main
// run: make -C ../ filemark && ../bin/filemark asdfasdf getOffset

import (
	"os"
	"bytes"
	"strconv"
	"io/ioutil"

	"logsync/log"
)

var (
	_ = log.Println
	FLAG_OFFSET = "setOffset:"
	FLAG_DONE = "setDone:"
)

func usage() {
	println("usage: filemark <file> <action> [params...]")
}

func main() {
	if len(os.Args) < 3 {
		usage()
		return
	}
	realFile := os.Args[1]
	action := os.Args[2]
	params := os.Args[3:]

	markFile := realFile + ".lsc"
	funcs := map[string] func(string, string, []string) {
		"getOffset": getOffset,
		"setOffset": setOffset,
		"offsetAppend": offsetAppend,
		"setDone": setDone,
		"getDone": getDone,
		"size": size,
	}

	f, ok := funcs[action]
	if ! ok {
		log.Exit("action", action, "not exists")
	}
	f(markFile, realFile, params)
}

func output(b []byte) {
	os.Stdout.Write(b)
}

func offsetAppend(mf, rf string, params []string) {
	getOffset(mf, rf, params)
	output([]byte{' '})
	size(mf, rf, params)
}

func size(mf, rf string, params []string) {
	stat, err := os.Stat(rf)
	if err != nil {
		output([]byte{'0'})
		return
	}
	size := stat.Size()
	output([]byte(strconv.FormatInt(size, 10)))
}

func setOffset(mf, rf string, params []string) {
	if len(params) < 1 {
		log.Exit("usage: setOffset <offset>")
	}
	WriteFile(mf, FLAG_OFFSET + params[0])
}

func getOffset(mf, rf string, params []string) {
	data, ok := OpenFile(mf)
	if ! ok {
		output([]byte{'0'})
		return
	}

	ret, ok := TailGrep(FLAG_OFFSET, data)
	if ! ok {
		output([]byte{'0'})
		return
	}
	output(ret)
}

func getDone(mf, rf string, params []string) {
	if len(params) < 2 {
		log.Exit("usage: getDone <tag> <offset>")
		return
	}
	tag := params[0]
	offset := params[1]

	data, ok := OpenFile(mf)
	if ! ok {
		output([]byte{'0'})
		return
	}
	_, ok = TailGrep(FLAG_DONE + tag + "." + offset, data)
	if ! ok {
		output([]byte{'0'})
		return
	}
	output([]byte{'1'})
}

func setDone(mf, rf string, params []string) {
	if len(params) < 2 {
		log.Exit("usage: getDone <tag> <offset>")
		return
	}
	tag := params[0]
	offset := params[1]

	WriteFile(mf, FLAG_DONE+tag+"."+offset)
}

func WriteFile(fpath, data string) (ok bool) {
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Error(err)
		return
	}
	_, err = f.WriteString(data + "\n")
	ok = err == nil
	return
}

func OpenFile(f string) (data []byte, ok bool) {
	data, err := ioutil.ReadFile(f)
	if err != nil && os.IsNotExist(err) {
		return
	}
	ok = true
	return
}

func TailGrep(flag string, data []byte) (ret []byte, ok bool) {
	idx := bytes.LastIndex(data, []byte(flag))
	if idx < 0 {
		return
	}
	data = data[idx+len(flag):]
	idx = bytes.Index(data, []byte{'\n'})
	if idx > 0 {
		data = data[:idx]
	}

	ok = true
	ret = data
	return
}
