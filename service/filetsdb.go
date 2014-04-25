package main
// run: make -C ../ filetsdb && time cat req.log | ./tsdbsum -s 300 | ../bin/filetsdb -o stdout2 -i ningbo/nb8/REQ/LOGGER/logger

import (
	"io"
	"os"
	"flag"
	"time"
	"bytes"
	"bufio"
	"strings"
	"strconv"
	"io/ioutil"

	"logsync/log"
)

var (
	output = flag.String("o", "", "output: [stdout|{path}]")
	info = flag.String("i", "", "{idc}/{host}/{logtype}/{stype}/{sname}")
	overwrite = flag.Bool("f", false, "overwrite if true else plus data")
)

func main() {
	flag.Parse()
	if !checkArgs() {
		os.Exit(1)
	}
	extraTag, tagsInfo := getExtraTag()
	data := make(map[string] map[string]string, 1440)
	buf := bufio.NewReader(os.Stdin)
	for {
		l, _, err := buf.ReadLine()
		if err != nil {
			if err != io.EOF {
				log.Exit(err)
			}
			break
		}

		tsdb := NewTsdbData(l)
		tableTimestamp := tsdb.GetTableAndTimestamp()
		m, ok := data[tableTimestamp]
		if !ok {
			m = make(map[string] string, 1024)
			data[tableTimestamp] = m
		}
		m[tsdb.GetTags() + extraTag] = tsdb.GetVal()
	}

	// map[table+timestamp] map[tag] val
	// k: table+timestamp
	// v: map[tag] val
	for tableTimestamp, tagVals := range data {
		tbl, timestampStr := splitTableTimestamp(tableTimestamp)

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Exit(err)
		}

		date := time.Unix(timestamp, 0).Format("2006-01-02")
		dirname := makeDestDir(date, tbl, tagsInfo)
		destPath := dirname + timestampStr

		if !*overwrite {
			mergeResultFromFile(destPath, tagVals)
		}

		WriteTagVal(destPath, tableTimestamp, tagVals)
	}
}

// -----------------------------------------------------------------------------

func checkArgs() bool {
	if *output == "" {
		log.Error("option -o is required!")
		return false
	}
	*output = strings.TrimRight(*output, "/") + "/"
	return true
}

// extraTag:
//     根据*info, 生成必备Tag，host=nb8 idc=ningbo stype=xx sname=xx1
//     当stype是*时(ccattee)，不添加stype
// destPath: {host}/{sname}
func getExtraTag() (extraTag, destPath string) {
	// info: {idc}/{host}/{logtype}/{stype}/{sname}
	sp := strings.Split(*info, "/")
	tags := make([]string, 0, 4)
	tags = append(tags, "host="+sp[1])
	tags = append(tags, "idc="+sp[0])
	if sp[3] != "*" {
		tags = append(tags, "stype="+sp[3])
	}
	tags = append(tags, "sname="+sp[4])
	if len(tags) > 0 {
		extraTag = " " + strings.Join(tags, " ")
	}
	destPath = sp[1]+"/"+sp[4]
	return
}


func splitTableTimestamp(tts string) (tbl, timestamp string) {
	l := len(tts)-11
	return tts[:l], tts[l+1:]
}

func makeDestDir(date, tbl, tagsInfo string) string {
	dirname := *output + date + "/" + tbl + "/" + tagsInfo + "/"
	err := os.MkdirAll(dirname, 0777)
	if err != nil {
		log.Exit(err)
	}
	return dirname
}

func mergeResultFromFile(destPath string, data map[string] string) {
	tmpdata, err := ioutil.ReadFile(destPath)
	if err != nil {
		return
	}

	tmpdata = bytes.TrimSpace(tmpdata)
	if len(tmpdata) == 0 {
		return
	}

	lineSp := strings.Split(string(tmpdata), "\n")
	for _, d := range lineSp {
		// d: put <table> <timestamp> <val> <tags...>
		sp := strings.Split(d, " ")
		tags := strings.Join(sp[4:], " ")
		lineValStr := sp[3]
		nowValStr, ok := data[tags]
		if !ok {
			data[tags] = lineValStr
			continue
		}

		lineVal, err := strconv.Atoi(lineValStr)
		if err != nil {
			log.Exit(err)
		}
		nowVal, err := strconv.Atoi(nowValStr)
		if err != nil {
			log.Exit(err)
		}
		data[tags] = strconv.Itoa(nowVal+lineVal)
	}
}

func WriteFile(destPath string, buf []byte) {
	err := ioutil.WriteFile(destPath, buf, 0666)
	if err != nil {
		log.Exit(err)
	}
}

func WriteTagVal(destPath, tableTimestamp string, tagVals map[string] string) {
	buf := bytes.NewBuffer(make([]byte, 0, len(tagVals)*100))
	for tags, val := range tagVals {
		buf.WriteString("put ")
		buf.WriteString(tableTimestamp)
		buf.WriteByte(' ')
		buf.WriteString(val)
		buf.WriteByte(' ')
		buf.WriteString(tags)
		buf.WriteByte('\n')
	}

	if *output == "stdout/" {
		buf.WriteTo(os.Stdout)
		return
	}

	WriteFile(destPath, buf.Bytes())
}

// TsdbData --------------------------------------------------------------------

type TsdbData struct {
	// <table> <time> <tags...> <val>
	buf []byte
	beforeTagIdx int
}

func NewTsdbData(buf []byte) (t *TsdbData) {
	t = new(TsdbData)
	t.buf = buf
	return
}

func (t *TsdbData) initBeforeTagIdx() {
	if t.beforeTagIdx != 0 {
		return
	}
	t.beforeTagIdx = bytes.Index(t.buf, []byte(" ")) + 11
}

func (t *TsdbData) GetTableAndTimestamp() string {
	t.initBeforeTagIdx()
	return string(t.buf[:t.beforeTagIdx])
}

func (t *TsdbData) GetTags() string {
	t.initBeforeTagIdx()
	return string(t.buf[t.beforeTagIdx+1:])
}

func (t *TsdbData) GetVal() string {
	lastIdx := bytes.LastIndex(t.buf, []byte(" "))
	return string(t.buf[lastIdx+1:])
}
