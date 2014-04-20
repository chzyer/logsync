package main
// run: go build % && time cat req.log | ./tsdbsum -s 300 | head

import (
	"io"
	"os"
	"bufio"
	"bytes"
	// "time"
	"log"
	"flag"
	"strconv"
)

var (
	_ = log.Println
	ts = flag.Int("s", 60, "time segment")
	timeSegment int64 = 60
)

// readline: q_all_service_req_num 1397551510 api=io.get delay=1m 3
func main() {
	flag.Parse()
	timeSegment = int64(*ts)
	data := make(map[string] int64, 1024)

	stdin := bufio.NewReader(os.Stdin)
	table := make([]int, 0, 10)
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

		// 110ms
		val, ok := getVal(line[idx+1:])
		if !ok {
			println("[ERROR][Atoi]", string(line))
			continue
		}

		// 110ms, 033-022
		var idx2 int
		for _, ci := range table {
			if line[ci] == ' ' && line[ci+1] == '1' {
				idx2 = ci
				break
			}
		}
		if idx2 == 0 {
			idx2 = bytes.Index(line, []byte(" "))
			table = append(table, idx2)
		}
		if idx2 < 0 {
			println("[ERROR][LastTime]", string(line))
			continue
		}

		// 200ms, 045-022
		ok = floor(line[idx2+1:idx2+11])
		if !ok {
			println("[ERROR][qiuyu]", string(line))
			continue
		}

		data[string(line[:idx])] += val
	}

	writeToOutput(data)
}

func writeToOutput(data map[string] int64) {
	buf := bytes.NewBuffer(make([]byte, 0, len(data)*100))
	for k, v := range data {
		buf.WriteString(k)
		buf.WriteString(" ")
		buf.WriteString(strconv.FormatInt(v, 10))
		buf.WriteByte('\n')
	}
	buf.WriteTo(os.Stdout)
}

func getVal(b []byte) (ret int64, ok bool) {
	for _, c := range b {
		ret = ret*10 + int64(c-'0')
	}
	ok = true
	return
}

// var now time.Time
// var a time.Duration
func floor(b []byte) (ok bool) {
	val, ok := getVal(b)
	if !ok {
		return
	}
	val = val/timeSegment*timeSegment
	strconv.AppendInt(b[:0], val, 10)
	return
}
