// Package bloomfilter is face-meltingly fast, thread-safe,
// marshalable, unionable, probability- and
// optimal-size-calculating Bloom filter in go
//
// https://github.com/steakknife/bloomfilter
//
// Copyright © 2014, 2015, 2018 Barry Allard
//
// MIT license
//
package bloomfilter

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"hash"
	"io"
)

func unmarshalBinaryHeader(r io.Reader) (k, n, m uint64, err error) {
	err = binary.Read(r, binary.LittleEndian, &k)
	if err != nil {
		return k, n, m, err
	}

	if k < KMin {
		return k, n, m, errK()
	}

	err = binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return k, n, m, err
	}

	err = binary.Read(r, binary.LittleEndian, &m)
	if err != nil {
		return k, n, m, err
	}

	if m < MMin {
		return k, n, m, errM()
	}

	debug("read bf k=%d n=%d m=%d\n", k, n, m)

	return k, n, m, err
}

func unmarshalBinaryBits(r io.Reader, m uint64) (bits []uint64, err error) {
	bits, err = newBits(m)
	if err != nil {
		return bits, err
	}
	bs := make([]byte, 8)
	for i := 0; i < len(bits); i++ {
		if _, err = r.Read(bs); err != nil {
			return bits, err
		}
		bits[i] = binary.LittleEndian.Uint64(bs)
	}
	return bits, err
}

func unmarshalBinaryKeys(r io.Reader, k uint64) (keys []uint64, err error) {
	keys = make([]uint64, k)
	err = binary.Read(r, binary.LittleEndian, keys)
	return keys, err
}

// hashingReader can be used to read from a reader, and simultaneously
// do a hash on the bytes that were read
type hashingReader struct {
	reader io.Reader
	hasher hash.Hash
	tot    int64
}

func (h *hashingReader) Read(p []byte) (n int, err error) {
	n, err = h.reader.Read(p)
	h.tot += int64(n)
	if err != nil {
		return n, err
	}
	h.hasher.Write(p)
	return n, err
}

// UnmarshalBinary converts []bytes into a Filter
// conforms to encoding.BinaryUnmarshaler
func (f *Filter) UnmarshalBinary(data []byte) (err error) {
	buf := bytes.NewBuffer(data)
	_, err = f.UnmarshalFromReader(buf)
	return err
}

func (f *Filter) UnmarshalFromReader(input io.Reader) (n int64, err error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	buf := &hashingReader{
		reader: input,
		hasher: sha512.New384(),
	}
	var k uint64
	k, f.n, f.m, err = unmarshalBinaryHeader(buf)
	if err != nil {
		return buf.tot, err
	}

	f.keys, err = unmarshalBinaryKeys(buf, k)
	if err != nil {
		return buf.tot, err
	}
	f.bits, err = unmarshalBinaryBits(buf, f.m)
	if err != nil {
		return buf.tot, err
	}

	// Only the hash remains to be read now
	// so abort the hasher at this point
	gotHash := buf.hasher.Sum(nil)
	expHash := make([]byte, sha512.Size384)
	err = binary.Read(buf, binary.LittleEndian, expHash)
	if err != nil {
		return buf.tot, err
	}
	if !bytes.Equal(gotHash, expHash) {
		debug("bloomfilter.UnmarshalBinary() sha384 hash failed:"+
			" actual %v  expected %v", gotHash, expHash)
		return buf.tot, errHash()
	}
	return buf.tot, nil
}
