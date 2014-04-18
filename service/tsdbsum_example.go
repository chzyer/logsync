package main
// run: go run % 2>req.log

import (
	"time"
	"math/rand"
	// "strconv"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func a() chan string {
	ch := make(chan string)
	data := map[string] []string {
		"api": {"io.get", "rs.ins", "a.get", "rs.aget", "pu.get", "cmd.run"},
		// "host": {"nb98", "zmt1", "bj12", "nb4", "zmt4"},
		// "idc": {"ningbo", "zhangmutou", "beijing"},
		// "service": {"IO", "RS", "PU", "A", "CMD"},
		"delay": {"1m", "10m", "1h"},
	}
	ptrs := make([]int, len(data))
	keys := make([]string, 0, len(data))
	for k, _ := range data {
		keys = append(keys, k)
	}
	go func() {
		br := false
		for !br {

			br = true
			for i, p := range ptrs {
				if p < len(data[keys[i]])-1 {
					br = false
					break
				}
			}

			tags := ""
			for i:=0; i<len(data); i++ {
				tags += " "+keys[i]+"="+data[keys[i]][ptrs[i]]
			}
			ch <- tags[1:]

			for i:=0; i<len(data); i++ {
				ptrs[i] ++
				if ptrs[i] != len(data[keys[i]]) {
					break
				}
				ptrs[i] = 0
			}

		}
		close(ch)
	}()
	return ch
}

func main() {
	tbl := "q_all_service_req_num"

	now := time.Now().Add(-3*24*time.Hour).Unix()
	n := time.Now().Unix()
	for now < n {
		now += int64(rand.Int()) % 60
		for i := range a() {
			println(tbl, now, i, rand.Int()%10)
		}
	}
	// for k, v := range data {
	// }
}
