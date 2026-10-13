package object

import (
	"fmt"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

// Stored form, identical on disk, in caches and on the wire, so that any tier
// can stream bytes straight from memory without recompressing:
//
//	[0] kind
//	[1] codec (0 = raw, 1 = zstd)
//	[2:] payload, possibly compressed
const (
	codecRaw  = 0
	codecZstd = 1
	// Payloads smaller than this are not worth compressing.
	minCompress = 64
)

var (
	zenc, _ = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)),
		zstd.WithLowerEncoderMem(true))
	zdec, _ = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)),
		zstd.WithDecoderMaxMemory(4<<30))
)

// Encode hashes and compresses a payload, returning its ID and stored form.
func Encode(k Kind, payload []byte) (ID, []byte) {
	id := Hash(k, payload)
	return id, Pack(k, payload)
}

// Pack produces the stored form of a payload.
func Pack(k Kind, payload []byte) []byte {
	if len(payload) >= minCompress {
		out := make([]byte, 2, 2+len(payload)/2)
		out[0], out[1] = byte(k), codecZstd
		out = zenc.EncodeAll(payload, out)
		if len(out) < len(payload)+2 {
			return out
		}
	}
	out := make([]byte, 2+len(payload))
	out[0], out[1] = byte(k), codecRaw
	copy(out[2:], payload)
	return out
}

// Unpack returns the kind and payload of a stored object.
func Unpack(stored []byte) (Kind, []byte, error) {
	if len(stored) < 2 {
		return 0, nil, fmt.Errorf("%w: short object", ErrCorrupt)
	}
	k := Kind(stored[0])
	switch stored[1] {
	case codecRaw:
		return k, stored[2:], nil
	case codecZstd:
		p, err := zdec.DecodeAll(stored[2:], nil)
		if err != nil {
			return 0, nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		return k, p, nil
	}
	return 0, nil, fmt.Errorf("%w: unknown codec %d", ErrCorrupt, stored[1])
}

// PayloadSize returns the uncompressed payload size of a stored object,
// reading only the zstd frame header when possible.
func PayloadSize(stored []byte) (uint64, error) {
	if len(stored) < 2 {
		return 0, fmt.Errorf("%w: short object", ErrCorrupt)
	}
	switch stored[1] {
	case codecRaw:
		return uint64(len(stored) - 2), nil
	case codecZstd:
		var h zstd.Header
		if err := h.Decode(stored[2:]); err == nil && h.HasFCS {
			return h.FrameContentSize, nil
		}
		_, p, err := Unpack(stored)
		return uint64(len(p)), err
	}
	return 0, fmt.Errorf("%w: unknown codec %d", ErrCorrupt, stored[1])
}

// Verify unpacks a stored object and checks it hashes to id.
func Verify(id ID, stored []byte) (Kind, []byte, error) {
	k, p, err := Unpack(stored)
	if err != nil {
		return 0, nil, err
	}
	if Hash(k, p) != id {
		return 0, nil, fmt.Errorf("%w: hash mismatch for %s", ErrCorrupt, id.Short())
	}
	return k, p, nil
}
