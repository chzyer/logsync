package main
// run: go build % && time cat req.log | ./tsdbsum -s 600 | ./sumfile -o /disk2 -host nb8 -idc ningbo -sname io1 -stype io

import (
	"os"
	"log"
	"bytes"
	"bufio"
	"strings"
	"flag"
	"strconv"
	"io/ioutil"
)

var (
	_ = ioutil.ReadAll
	_ = log.Println
	output = flag.String("o", "", "output")
	host = flag.String("host", "", "host")
	idc = flag.String("idc", "", "idc")
	sname = flag.String("sname", "", "sname")
	stype = flag.String("stype", "", "stype")
	overwrite = flag.Bool("f", false, "overwrite if true else plus data")
	extraTag = ""
	tagPath = ""
)

func init() {
	flag.Parse()
	if *output == "" {
		println("option -o is required!")
		os.Exit(-3)
	}
	*output = strings.TrimRight(*output, "/") + "/"

	tags := make([]string, 0, 4)
	if *host == "" {
		println("miss -host")
		os.Exit(1)
	}
	if *idc == "" {
		println("miss -idc")
		os.Exit(1)
	}
	if *sname == "" {
		println("miss -sname")
		os.Exit(1)
	}
	if *stype == "" {
		println("miss -stype")
		os.Exit(1)
	}
	tags = append(tags, "host="+*host)
	tags = append(tags, "idc="+*idc)
	tags = append(tags, "sname="+*sname)
	tags = append(tags, "stype="+*stype)
	if len(tags) > 0 {
		extraTag = " " + strings.Join(tags, " ")
		tagPath = strings.Join(tags, ".") + "/"
	}

}

type KV struct {
	Key string
	Val string
}

func main() {
	data := make(map[string] map[string]string, 1440)
	buf := bufio.NewReader(os.Stdin)
	for {
		l, _, err := buf.ReadLine()
		if err != nil {
			break
		}

		idx := bytes.Index(l, []byte(" "))
		l[idx] = '/'

		lastIdx := bytes.LastIndex(l, []byte(" "))

		path := string(l[:idx+11])
		m, ok := data[path]
		if !ok {
			m = make(map[string] string, 1024)
			data[path] = m
		}
		m[string(l[idx+12: lastIdx])] = string(l[lastIdx+1:])
	}

	for k, v := range data {
		dirname := *output + k[:len(k)-10] + tagPath
		fname := k[len(k)-10:]
		path := dirname + fname
		err := os.MkdirAll(dirname, 0777)
		if err != nil {
			println(err.Error())
			continue
		}

		if !*overwrite {
			if tmpdata, err := ioutil.ReadFile(path); err == nil {
				for _, d := range strings.Split(string(tmpdata), "\n") {
					idx := strings.Index(d, " ")
					if idx < 0 {
						continue
					}
					key := d[idx+1:]
					myval := d[:idx]
					val, ok := v[key]
					if !ok {
						v[key] = myval
					} else {
						myvals, err := strconv.Atoi(myval)
						if err != nil {
							continue
						}
						vals, err := strconv.Atoi(val)
						if err != nil {
							continue
						}
						v[key] = strconv.Itoa(vals+myvals)
					}
				}
			}
		}

		buf := bytes.NewBuffer(make([]byte, 0, len(v)*100))
		for k, vv := range v {
			buf.WriteString(vv)
			buf.WriteByte(' ')
			buf.WriteString(k)
			buf.WriteByte('\n')
		}

		err = ioutil.WriteFile(path, buf.Bytes(), 0666)
		if err != nil {
			println(err.Error())
		}
	}
}
