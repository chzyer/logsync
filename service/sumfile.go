package main
// run: go build % && time cat req.log | ./tsdbsum -s 300 | ./sumfile -o stdout -i ningbo/nb8/REQ/LOGGER/logger

import (
	"io"
	"os"
	"log"
	"flag"
	"time"
	"bytes"
	"bufio"
	"strings"
	"strconv"
	"io/ioutil"
)

var (
	_ = ioutil.ReadAll
	_ = log.Println
	output = flag.String("o", "", "output")
	info = flag.String("i", "", "{idc}/{host}/{logtype}/{stype}/{sname}")
	overwrite = flag.Bool("f", false, "overwrite if true else plus data")
	pathInfo = ""
	extraTag = ""
	tagPath = ""
)

func initArgs() {
	flag.Parse()
	if *output == "" {
		log.Println("option -o is required!")
		os.Exit(1)
	}
	*output = strings.TrimRight(*output, "/") + "/"

	sp := strings.Split(*info, "/")
	tags := make([]string, 0, 4)
	tags = append(tags, "host="+sp[1])
	tags = append(tags, "idc="+sp[0])
	if sp[3] == "*" {
		tags = append(tags, "stype=LOGGER")
	}
	tags = append(tags, "sname="+sp[4])
	if len(tags) > 0 {
		extraTag = " " + strings.Join(tags, " ")
		tagPath = strings.Join(tags, ".") + "/"
	}
	pathInfo = sp[1]+"/"+sp[4]
}

// <table> <time> <tags...> <val>
func main() {
	initArgs()
	data := make(map[string] map[string]string, 1440)
	buf := bufio.NewReader(os.Stdin)
	for {
		l, _, err := buf.ReadLine()
		if err != nil {
			if err != io.EOF {
				log.Println(err)
				os.Exit(1)
			}
			break
		}

		idx := bytes.Index(l, []byte(" "))
		lastIdx := bytes.LastIndex(l, []byte(" "))

		path := string(l[:idx+11])
		m, ok := data[path]
		if !ok {
			m = make(map[string] string, 1024)
			data[path] = m
		}
		m[string(l[idx+12: lastIdx]) + extraTag] = string(l[lastIdx+1:])
	}

	for k, v := range data {
		timeStr := k
		fname := k[len(k)-10:]
		timestamp, err := strconv.ParseInt(fname, 10, 64)
		if err != nil {
			log.Println("[ERROR]", err)
			continue
		}
		date := time.Unix(timestamp, 0).Format("2006-01-02")
		dirname := *output + date + "/" + timeStr[:len(timeStr)-11] + "/" + pathInfo + "/"
		path := dirname + fname
		err = os.MkdirAll(dirname, 0777)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		if !*overwrite {
			if tmpdata, err := ioutil.ReadFile(path); err == nil {
				for _, d := range strings.Split(strings.TrimSpace(string(tmpdata)), "\n") {
					data := strings.Split(d, " ")
					key := strings.Join(data[4:], " ")
					myval := data[3]
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
			buf.WriteString("put ")
			buf.WriteString(timeStr)
			buf.WriteByte(' ')
			buf.WriteString(vv)
			buf.WriteByte(' ')
			buf.WriteString(k)
			buf.WriteByte('\n')
		}
		if *output == "stdout/" {
			buf.WriteTo(os.Stdout)
			continue
		}

		err = ioutil.WriteFile(path, buf.Bytes(), 0666)
		if err != nil {
			log.Println(err)
		}
	}
}
