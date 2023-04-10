package main

import (
	"bytes"
	"fmt"

	"6.824/labgob"
	"6.824/shardkv"
)

func getmap(k, v string) map[string]string {
	mp := make(map[string]string)
	mp[k] = v
	return mp
}
func encode(s []shardkv.Shard) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(s)
	data := w.Bytes()
	return data
}
func decode(data []byte) []shardkv.Shard {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	s := []shardkv.Shard{}
	if d.Decode(&s) != nil {
		//   error...
	}
	return s
}
func main() {
	s := make([]shardkv.Shard, 3)
	s[0] = shardkv.Shard{
		KVDB:  getmap("a", "A"),
		State: shardkv.Serving,
	}
	s[1] = shardkv.Shard{
		KVDB:  getmap("aa", "Aa"),
		State: shardkv.Serving,
	}
	s[2] = shardkv.Shard{
		KVDB:  getmap("aaa", "Aa"),
		State: shardkv.Serving,
	}
	s[1].KVDB = nil
	data := encode(s)
	fmt.Printf("datasize=%v\n", len(data))
	t := decode(data)
	fmt.Println(t)
	if t[1].KVDB == nil {
		fmt.Printf("is nil\n")
	}
}
