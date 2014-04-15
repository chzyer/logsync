package main
// run: make -C ../ && ../bin/server -c ../server.conf
// build: make -C ../

import (
	"net"
	"flag"
	"io/ioutil"
	"logsync/log"
	"encoding/json"

	"logsync/svrdir"
	"logsync/svrfile"
)

var (
	confpath = flag.String("c", "logsync.conf", "conf file path")
)

type Conf struct {
	Path string `json:"path"`
	Listen string `json:"listen"`
}

type Service struct {
	*Conf
	ln net.Listener
	file *svrfile.SvrFile
}

func NewService(c *Conf) (s *Service, err error) {
	s = &Service{Conf: c}
	ln, err := net.Listen("tcp", s.Listen)
	if err != nil {
		return
	}
	s.ln = ln
	return
}

func (s *Service) Run() (err error) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Error(err)
			continue
		}
		go svfdir.HandleConn(conn)
	}
}

func main() {
	flag.Parse()
	data, err := ioutil.ReadFile(*confpath)
	if err != nil {
		log.Exit(err)
	}
	conf := new(Conf)
	err = json.Unmarshal(data, conf)
	if err != nil {
		log.Exit(err)
	}

	s, err := NewService(conf)
	if err != nil {
		log.Exit(err)
	}
	err = s.Run()
	if err != nil {
		log.Exit(err)
	}
}
