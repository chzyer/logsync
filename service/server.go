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
	WritePath string `json:"write_path"`
	Listen string `json:"listen"`
	Owner string `json:"owner"` // 留空表示试用当前用户
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
	s.file, err = svrfile.NewSvrFile(c.Owner)
	if err != nil {
		return
	}
	return
}

func (s *Service) Run() (err error) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Error(err)
			continue
		}
		go svrdir.ServeConn(s.file, conn)
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
