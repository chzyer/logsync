package main
// run: make -C ../ client && ../bin/client -c ../client.conf

import (
	"os"
	"flag"
	"io/ioutil"
	"encoding/json"

	"logsync/log"
	"logsync/remotedir"
	"logsync/localdir"
)

var (
	confpath = flag.String("c", "client.conf", "conf file")
)

/*
path: {
	"$path": {
		"name": "image1",
		"type": "image",
		"path": "path",
	}
}
*/

type XService struct {
	Name string `json:"name"`
	Type string `json:"type"`
	UseInodeName bool `json:"use_inode_name"`
	Log map[string] string `json:"log"` // map[type] path
}

type Conf struct {
	Host string `json:"host"` // getHostname if empty
	Service []XService `json:"service"`
	ServerAddr string `json:"server_addr"`
}

type Client struct {
	*Conf
}

func NewClient(c *Conf) (client *Client) {
	client = &Client{Conf:c}
	log.Obj("sync directory...", client.Service)
	return
}

func (c *Client) syncDir(service XService, logType string, errChan chan error) {
	var err error
	defer func() {
		if err != nil {
			errChan <- err
		}
	}()

	remote, err := remotedir.NewDir(c.ServerAddr, &remotedir.Info {
		Host: c.Host,
		LogType: logType,
		ServiceName: service.Name,
		ServiceType: service.Type,
	})
	if err != nil {
		return
	}

	p := service.Log[logType]
	dir, err := localdir.NewDir(p, service.UseInodeName)
	if err != nil {
		return
	}
	err = dir.Sync(remote)
}

func (c *Client) Run() (err error) {
	errChan := make(chan error)
	for _, service := range c.Service {
		for logType, _ := range service.Log {
			go c.syncDir(service, logType, errChan)
		}
	}
	err = <-errChan
	return
}

func main() {
	log.Info("started")
	flag.Parse()
	data, err := ioutil.ReadFile(*confpath)
	if err != nil {
		log.Exit(err)
	}

	conf := new(Conf)
	if err=json.Unmarshal(data, conf); err != nil {
		log.Exit(string(data), err)
	}
	if conf.Host == "" {
		conf.Host, _ = os.Hostname()
	}

	c := NewClient(conf)
	if err=c.Run(); err != nil {
		log.Exit(err)
	}
}
