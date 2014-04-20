package main
// run: go build % && time cat req.log | ./tsdbsum -s 300 > /dev/null

import (
	"io"
	"os"
	"bufio"
	"bytes"
	"strings"
	// "time"
	"log"
	"flag"
	"strconv"
)

var (
	_ = log.Println
	ts = flag.Int("s", 60, "time segment")
	timeSegment int64 = 60
	output = flag.String("o", "", "output")
	tsdbStyle = flag.Bool("tsdb", false, "output with openTsdb style")
	extraTag = ""
)

// readline: q_all_service_req_num 1397551510 api=io.get delay=1m 3
func main() {
	flag.Parse()
	timeSegment = int64(*ts)
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

		ok = floor(line[idx2+1:idx2+11])
		if !ok {
			println("[ERROR][qiuyu]", string(line))
			continue
		}

		key := string(line[:idx])
		data[key] += val
	}

	writeToOutput(data)
}

func writeToOutput(data map[string] int64) {
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

func pow(e int) (ret int64) {
	ret = 1
	for i:=0; i<e; i++ {
		ret *= 10
	}
	return ret
}

func getVal(b []byte) (ret int64, ok bool) {
	length := len(b)
	if length == 0 {
		return
	}
	negative := b[0] == '-'
	if negative {
		length -= 1
		b = b[1:]
	}
	for i:=0; i<length; i++ {
		c := b[i]
		if b[i] > '9' || b[i] < '0' {
			return
		}
		ret += int64(c-'0')*pow(length-i-1)
	}
	ok = true
	if negative {
		ret = -ret
	}
	return
}

func floor(b []byte) (ok bool) {
	val, ok := getVal(b)
	if !ok {
		return
	}
	val = val/timeSegment*timeSegment
	strconv.AppendInt(b[:0], val, 10)
	return
}
