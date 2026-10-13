package store

import (
	"encoding/binary"
	"errors"
)

// Delta encoding for server-side storage. A delta is a sequence of ops that
// rebuild a target from a base:
//
//	0x00 len  bytes      insert literal bytes
//	0x01 off  len        copy len bytes from base[off:]
//
// Matching uses 16-byte blocks of the base sampled at aligned offsets, then
// extends matches in both directions, which suits both text and binary
// objects such as trees (unchanged entries copy through).
const deltaBlock = 16

var errBadDelta = errors.New("corrupt delta")

func blockHash(b []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range b[:deltaBlock] {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return h
}

// makeDelta returns a delta from base to target, or nil if base is too
// small to be useful.
func makeDelta(base, target []byte) []byte {
	if len(base) < deltaBlock || len(target) < deltaBlock {
		return nil
	}
	idx := make(map[uint64]int32, len(base)/deltaBlock+1)
	for i := 0; i+deltaBlock <= len(base); i += deltaBlock {
		h := blockHash(base[i:])
		if _, ok := idx[h]; !ok {
			idx[h] = int32(i)
		}
	}
	out := make([]byte, 0, len(target)/8+16)
	insert := func(b []byte) {
		if len(b) == 0 {
			return
		}
		out = append(out, 0)
		out = binary.AppendUvarint(out, uint64(len(b)))
		out = append(out, b...)
	}
	ins := 0
	i := 0
	for i+deltaBlock <= len(target) {
		off, ok := idx[blockHash(target[i:])]
		if !ok || string(base[off:int(off)+deltaBlock]) != string(target[i:i+deltaBlock]) {
			i++
			continue
		}
		s, bs := i, int(off)
		for s > ins && bs > 0 && target[s-1] == base[bs-1] {
			s--
			bs--
		}
		e, be := i+deltaBlock, int(off)+deltaBlock
		for e < len(target) && be < len(base) && target[e] == base[be] {
			e++
			be++
		}
		insert(target[ins:s])
		out = append(out, 1)
		out = binary.AppendUvarint(out, uint64(bs))
		out = binary.AppendUvarint(out, uint64(e-s))
		i, ins = e, e
	}
	insert(target[ins:])
	return out
}

// applyDelta rebuilds the target of size n from base and delta.
func applyDelta(base, delta []byte, n uint64) ([]byte, error) {
	out := make([]byte, 0, n)
	for len(delta) > 0 {
		op := delta[0]
		delta = delta[1:]
		switch op {
		case 0:
			l, k := binary.Uvarint(delta)
			if k <= 0 || uint64(len(delta)-k) < l {
				return nil, errBadDelta
			}
			out = append(out, delta[k:k+int(l)]...)
			delta = delta[k+int(l):]
		case 1:
			off, k := binary.Uvarint(delta)
			if k <= 0 {
				return nil, errBadDelta
			}
			delta = delta[k:]
			l, k := binary.Uvarint(delta)
			if k <= 0 || off+l > uint64(len(base)) {
				return nil, errBadDelta
			}
			delta = delta[k:]
			out = append(out, base[off:off+l]...)
		default:
			return nil, errBadDelta
		}
	}
	if uint64(len(out)) != n {
		return nil, errBadDelta
	}
	return out, nil
}
