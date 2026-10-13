package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
)

func main() {
	db, err := pebble.Open(os.Args[1], &pebble.Options{ReadOnly: true})
	if err != nil {
		panic(err)
	}
	it, _ := db.NewIter(nil)
	sizes := map[string]int64{}
	counts := map[string]int64{}
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		cat := string(k[0])
		if k[0] == 'o' {
			cat = "o:" + string(it.Value()[0])
		}
		if k[0] == 'd' {
			for i, c := range k {
				if c == 0 {
					cat = "d:" + string(k[1:i])
					break
				}
			}
		}
		sizes[cat] += int64(len(k) + len(it.Value()))
		counts[cat]++
	}
	for c, s := range sizes {
		fmt.Printf("%-10s %8d objs %8.1f MB\n", c, counts[c], float64(s)/1e6)
	}
	m := db.Metrics()
	fmt.Printf("disk usage %.1f MB\n", float64(m.DiskSpaceUsage())/1e6)
}

func init() {
	if len(os.Args) > 2 && os.Args[2] == "codecs" {
		db, err := pebble.Open(os.Args[1], &pebble.Options{ReadOnly: true})
		if err != nil {
			panic(err)
		}
		it, _ := db.NewIter(&pebble.IterOptions{LowerBound: []byte{'o'}, UpperBound: []byte{'p'}})
		type k struct{ kind, codec byte }
		n := map[k]int{}
		sz := map[k]int{}
		for it.First(); it.Valid(); it.Next() {
			v := it.Value()
			n[k{v[0], v[1]}]++
			sz[k{v[0], v[1]}] += len(v)
		}
		for kk, c := range n {
			fmt.Printf("kind %c codec %d: %7d objs %8.1f MB avg %5d B\n", kk.kind, kk.codec, c, float64(sz[kk])/1e6, sz[kk]/c)
		}
		os.Exit(0)
	}
}
