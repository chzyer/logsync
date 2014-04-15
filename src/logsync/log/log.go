package log

import (
	"runtime"
	"path"
	"strconv"
	"os"
	"fmt"
	"strings"
	go_log "log"
)

var level = 1
type Logger struct {
	depth int
}

func NewLogger(l int) (Logger) {
	return Logger {l}
}

var std = Logger {1}
var (
	Println = std.Println
	Infof = std.Infof
	Info = std.Info
	Debug = std.Debug
	Error = std.Error
	Warn = std.Warn
	Stack = std.Stack
	Panic = std.Panic
	Exit = std.Exit
	Obj = std.Obj
)

func init() {
	for _, i := range os.Args {
		if i == "-v" { level = 0 }
	}
}

func (l Logger) Println(o ...interface{}) { l.output("", o) }
func (l Logger) Infof(f string, info ...interface{}) { l.output("INFO", []interface{}{fmt.Sprintf(f, info...)}) }
func (l Logger) Info(info ...interface{}) { l.output("INFO", info) }
func (l Logger) Debug(info ...interface{}) {
	if level > 0 { return }
	l.output("DEBUG", info)
}
func (l Logger) Error(info ...interface{}) { l.output("ERROR", info) }
func (l Logger) Warn(info ...interface{}) { l.output("WARN", info) }
func (l Logger) Panic(info interface{}) {
	l.output("PANIC", []interface{}{info})
	panic(info)
}
func (l Logger) Exit(info interface{}) {
	l.output("EXIT", []interface{}{info})
	os.Exit(1)
}
func (l Logger) Obj(o ...interface{}) {
	if len(o) == 0 {
		return
	}
	layout := strings.Repeat(" `%+v`", len(o))
	l.output("OBJ", []interface{}{fmt.Sprintf(layout[1:], o...)})
}
func (l Logger) Stack() []byte {
	a := make([]byte, 1024*1024)
	n := runtime.Stack(a, true)
	return a[:n]
}

func (l Logger) output(tag string, info []interface{}) {
	pc, f, line, _ := runtime.Caller(2 + l.depth)
	name := runtime.FuncForPC(pc).Name()
	name = path.Base(name)
	f = path.Base(f)
	if tag != "" {
		tag = "["+tag+"]"
	}
	info = append([]interface{}{tag + "[" + f + ":" + strconv.Itoa(line) + "]["+name+"]"}, info...)
	go_log.Println(info...)
}
