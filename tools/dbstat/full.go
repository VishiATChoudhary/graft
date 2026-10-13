package main

import (
	"crypto/sha1"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
)

func init() {
	if len(os.Args) > 2 && os.Args[2] == "fullblobs" {
		db, err := pebble.Open(os.Args[1], &pebble.Options{ReadOnly: true})
		if err != nil {
			panic(err)
		}
		dict, closer, err := db.Get([]byte("mdict/1"))
		var ddec *zstd.Decoder
		if err == nil {
			ddec, _ = zstd.NewReader(nil, zstd.WithDecoderDicts(append([]byte{}, dict...)))
			closer.Close()
		}
		dec, _ := zstd.NewReader(nil)
		it, _ := db.NewIter(&pebble.IterOptions{LowerBound: []byte{'o'}, UpperBound: []byte{'p'}})
		for it.First(); it.Valid(); it.Next() {
			v := it.Value()
			if v[0] != 'b' || (v[1] != 1 && v[1] != 2) {
				continue
			}
			var p []byte
			if v[1] == 1 {
				p, err = dec.DecodeAll(v[2:], nil)
			} else {
				b := v[2:]
				for i := 0; i < 2; i++ { // skip dict no and size uvarints
					for b[0]&0x80 != 0 {
						b = b[1:]
					}
					b = b[1:]
				}
				p, err = ddec.DecodeAll(b, nil)
			}
			if err != nil {
				continue
			}
			h := sha1.New()
			fmt.Fprintf(h, "blob %d\x00", len(p))
			h.Write(p)
			fmt.Printf("%x %d %d\n", h.Sum(nil), len(p), len(v))
		}
		os.Exit(0)
	}
}
