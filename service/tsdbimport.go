package main
// run: go run % -f /disk2/q_all_service_req_num/host=nb8.idc=ningbo.sname=io1.stype=io/1397500800

import (
	"os"
	"io"
	"net"
	"flag"
	"bytes"
	"strings"
	"io/ioutil"
)

var (
	path = flag.String("f", "", "path")
	writeTo = flag.String("w", "stdout", "write to")
	conf []string
	writer io.Writer = os.Stdout
)

func init() {
	flag.Parse()
	conf = strings.Split(*path, "/")
	if len(conf) < 3 {
		println("invalid path")
		os.Exit(1)
	}
	conf = conf[len(conf)-3:]
	conf[1] = strings.Replace(conf[1], ".", " ", -1)

	if *writeTo != "stdout" {
		conn, err := net.Dial("tcp", *writeTo)
		if err != nil {
			println(err.Error())
			os.Exit(1)
		}
		writer = conn
	}
}

func main() {
	data, err := ioutil.ReadFile(*path)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	
	buf := bytes.NewBuffer(make([]byte, 0, len(lines) * 200))
	for _, l := range lines {
		buf.WriteString("put ")
		buf.WriteString(conf[0])
		buf.WriteString(" ")
		buf.WriteString(conf[2])
		buf.WriteString(" ")
		buf.WriteString(l)
		buf.WriteString(" ")
		buf.WriteString(conf[1])
		buf.WriteByte('\n')
	}
	os.Stdout.Write(buf.Bytes())
}

