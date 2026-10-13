package store

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestDeltaRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 2000; iter++ {
		base := make([]byte, r.Intn(5000))
		r.Read(base)
		target := append([]byte{}, base...)
		for k := r.Intn(6); k > 0 && len(target) > 0; k-- {
			pos := r.Intn(len(target))
			switch r.Intn(3) {
			case 0:
				ins := make([]byte, r.Intn(100))
				r.Read(ins)
				target = append(target[:pos], append(ins, target[pos:]...)...)
			case 1:
				end := min(len(target), pos+r.Intn(200))
				target = append(target[:pos], target[end:]...)
			case 2:
				target[pos] ^= 0xff
			}
		}
		d := makeDelta(base, target)
		if d == nil {
			continue
		}
		got, err := applyDelta(base, d, uint64(len(target)))
		if err != nil || !bytes.Equal(got, target) {
			t.Fatalf("iter %d: round trip failed: %v", iter, err)
		}
		if len(base) > 1000 && len(d) > len(target)/2+700 {
			t.Fatalf("iter %d: delta too large %d for target %d", iter, len(d), len(target))
		}
	}
}
