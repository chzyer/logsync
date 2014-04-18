package main
// run: go build % && time cat req.log | ./tsdbsum -s 300 -host nb8 -stype image

import (
	"io"
	"os"
	"bufio"
	"bytes"
	"strings"
	// "log"
	"flag"
	"strconv"
)

var (
	ts = flag.Int("s", 60, "time segment")
	timeSegment int64 = 60
	output = flag.String("o", "", "output")
	host = flag.String("host", "", "host")
	idc = flag.String("idc", "", "idc")
	sname = flag.String("sname", "", "sname")
	stype = flag.String("stype", "", "stype")
	tsdbStyle = flag.Bool("tsdb", false, "output with openTsdb style")
	extraTag = ""
)

func init() {
	flag.Parse()
	timeSegment = int64(*ts)
	tags := make([]string, 0, 4)
	if *host != "" {
		tags = append(tags, "host="+*host)
	}
	if *idc != "" {
		tags = append(tags, "idc="+*idc)
	}
	if *sname != "" {
		tags = append(tags, "sname="+*sname)
	}
	if *stype != "" {
		tags = append(tags, "stype="+*stype)
	}
	if len(tags) > 0 {
		extraTag = " " + strings.Join(tags, " ")
	}
}

func pow(e int) (ret int64) {
	ret = 1
	for i:=0; i<e; i++ {
		ret *= 10
	}
	return ret
}

func getVal(b []byte) (ret int64, ok bool) {
	length := len(b)
	for i:=0; i<length; i++ {
		c := b[i]
		if b[i] > '9' || b[i] < '0' {
			return
		}
		ret += int64(c-'0')*pow(length-i-1)
	}
	ok = true
	return
}

func qiuyu(b []byte) (ok bool) {
	val, ok := getVal(b)
	if !ok {
		return
	}
	val = val/timeSegment*timeSegment
	strconv.AppendInt(b[:0], val, 10)
	return
}

func main() {
	data := make(map[string] int64, 1024)

	stdin := bufio.NewReader(os.Stdin)
	var err error
	var line []byte
	for {
		line, _, err = stdin.ReadLine()
		if err != nil {
			if err != io.EOF {
				println(err.Error())
			}
			break
		}

		idx := bytes.LastIndex(line, []byte(" "))
		if idx < 0 {
			println("[ERROR][LastI]", string(line))
			continue
		}

		val, ok := getVal(line[idx+1:])
		if !ok {
			println("[ERROR][Atoi]", string(line))
			continue
		}

		idx2 := bytes.Index(line, []byte(" "))
		if idx2 < 0 {
			println("[ERROR][LastTime]", string(line))
			continue
		}

		ok = qiuyu(line[idx2+1:idx2+11])
		if !ok {
			println("[ERROR][qiuyu]", string(line))
			continue
		}

		key := string(line[:idx])
		data[key] = data[key] + val
	}

	buf := bytes.NewBuffer(make([]byte, 0, len(data)*100))
	if *tsdbStyle {
		for k, v := range data {
			idx := strings.Index(k, " ")
			if idx < 0 {
				println("[ERROR][OUT]", k, v)
				continue
			}
			idx2 := strings.LastIndex(k, " ")
			if idx2 < 0 {
				println("[ERROR][OUT2]", k, v)
				continue
			}
			buf.WriteString("put ")
			buf.WriteString(k[:idx])
			buf.WriteString(k[idx2:])
			buf.WriteString(" ")
			buf.WriteString(strconv.FormatInt(v, 10))
			buf.WriteString(k[idx:idx2])
			buf.WriteString(extraTag)
			buf.WriteByte('\n')
		}
	} else {
		for k, v := range data {
			buf.WriteString(strconv.FormatInt(v, 10))
			buf.WriteString(" ")
			buf.WriteString(k)
			buf.WriteString(extraTag)
			buf.WriteByte('\n')
		}
	}

	buf.WriteTo(os.Stdout)
}
