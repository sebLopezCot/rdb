// This is a very basic example of a program that implements rdb.decoder and
// outputs a human readable diffable dump of the rdb file.
package main

import (
	"fmt"
	"os"

	"github.com/seblopezcot/rdb"
	"github.com/seblopezcot/rdb/nopdecoder"
)

type decoder struct {
	db       int
	i        int
	keycount int
	keyset   map[string]struct{}
	nopdecoder.NopDecoder
}

func (p *decoder) StartDatabase(n int) {
	p.db = n
  fmt.Println("DB SIZE: ", n)
}

func (p *decoder) Set(key, value []byte, expiry int64) {
	if _, ok := p.keyset[string(key)]; !ok {
		p.keycount++
		p.keyset[string(key)] = struct{}{}
	}
	fmt.Printf("Set: db=%d %q -> %q\n", p.db, key, value)
}

func (p *decoder) Hset(key, field, value []byte) {
	if _, ok := p.keyset[string(key)]; !ok {
		p.keycount++
		p.keyset[string(key)] = struct{}{}
	}
	fmt.Printf("Hset: db=%d %q . %q -> %q\n", p.db, key, field, value)
}

func (p *decoder) Sadd(key, member []byte) {
	if _, ok := p.keyset[string(key)]; !ok {
		p.keycount++
		p.keyset[string(key)] = struct{}{}
	}
	fmt.Printf("Sadd: db=%d %q { %q }\n", p.db, key, member)
}

func (p *decoder) StartList(key []byte, length, expiry int64) {
  fmt.Printf("StartList: %v, length=%v\n", string(key), length)
  p.i = 0
}

func (p *decoder) Rpush(key, value []byte) {
	if _, ok := p.keyset[string(key)]; !ok {
		p.keycount++
		p.keyset[string(key)] = struct{}{}
	}
	fmt.Printf("Rpush: db=%d %q[%d] -> %q\n", p.db, key, p.i, value)
	p.i++
}

func (p *decoder) StartHash(key []byte, length, expiry int64) {
  fmt.Printf("STARTED HASH: %v, length=%v", string(key), length)
}

func (p *decoder) StartZSet(key []byte, cardinality, expiry int64) {
  fmt.Println("STARTED: ", string(key))
  p.i = 0
}

func (p *decoder) Zadd(key []byte, score float64, member []byte) {
	if _, ok := p.keyset[string(key)]; !ok {
		p.keycount++
		p.keyset[string(key)] = struct{}{}
	}
	fmt.Printf("Zadd: db=%d %q[%d] -> {%q, score=%g}\n", p.db, key, p.i, member, score)
	p.i++
}

func maybeFatal(err error) {
	if err != nil {
		fmt.Printf("Fatal error: %s\n", err)
		os.Exit(1)
	}
}

func main() {
	f, err := os.Open(os.Args[1])
	maybeFatal(err)
	d := decoder{
		keyset: make(map[string]struct{}),
	}
	err = rdb.Decode(f, &d)
	maybeFatal(err)
	fmt.Printf("Key count is %v\n", d.keycount)
}
