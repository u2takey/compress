package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
)

const (
	Simple8bMaxInt64 uint64 = 2305843009213693951
)

type EncodeType = byte

const (
	EncodeTypeRawInt64   EncodeType = 0
	EncodeTypeSimple8b   EncodeType = 1
	EncodeTypeRawFloat64 EncodeType = 2
	EncodeTypeXor        EncodeType = 3
)

var (
	bitPerInteger      = []int{0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60}
	selectorToElements = []int{240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}
	bitToSelector      = []int{0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
		14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
		15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15}
)

// Compress Integer (Simple8B).
func CompressInt64(input []int64) []byte {
	if len(input) > math.MaxUint32 {
		panic("can only compress len < 4294967295")
	}
	nElements, i, buf := len(input), 0, bytes.NewBuffer(nil)
	var prevValue int64 = 0
	var byteLimit, usedByte = 8*nElements + 5, 1

	_ = binary.Write(buf, binary.BigEndian, EncodeTypeSimple8b)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(input)))

	for i < nElements {
		selector, bit, elemCountSingleRound := 0, 0, 0
		prevValueTmp := prevValue

		for j := i; j < nElements; j++ {
			currValue := input[j]
			// Get difference.
			if !SafeAdd(currValue, -prevValueTmp) {
				goto CopyAndExit
			}
			// Zigzag encode diff.
			v := currValue - prevValueTmp
			zigzagValue := uint64((v >> 63) ^ (v << 1))
			if zigzagValue >= Simple8bMaxInt64 {
				goto CopyAndExit
			}

			tmpBit := 0
			if zigzagValue != 0 {
				tmpBit = 64 - bits.LeadingZeros64(zigzagValue)
			}

			if elemCountSingleRound+1 <= selectorToElements[selector] && elemCountSingleRound+1 <= selectorToElements[bitToSelector[tmpBit]] {
				// If can hold another one.
				if selector <= bitToSelector[tmpBit] {
					selector = bitToSelector[tmpBit]
				}
				elemCountSingleRound += 1
				bit = bitPerInteger[selector]
			} else {
				// if cannot hold another one.
				for elemCountSingleRound < selectorToElements[selector] {
					selector += 1
				}
				elemCountSingleRound = selectorToElements[selector]
				bit = bitPerInteger[selector]
				break
			}
			prevValueTmp = currValue
		}
		//
		var bufferUint64 uint64 = 0
		bufferUint64 |= uint64(selector)
		for k := 0; k < elemCountSingleRound; k++ {
			currValue := input[i]
			zigzagValue := ZigZagEncode(currValue - prevValue)
			bufferUint64 |= (zigzagValue & ((uint64(1) << bit) - 1)) << (bit*k + 4)
			i += 1
			if i >= nElements {
				break
			}
			prevValue = currValue
		}
		if usedByte+8 > byteLimit {
			goto CopyAndExit
		}
		_ = binary.Write(buf, binary.BigEndian, bufferUint64)
	}
	return buf.Bytes()
CopyAndExit:
	buf = bytes.NewBuffer(nil)
	_ = binary.Write(buf, binary.BigEndian, EncodeTypeRawInt64)
	for _, a := range input {
		_ = binary.Write(buf, binary.BigEndian, a)
	}
	return buf.Bytes()
}

func DeCompressInt64(input []byte) ([]int64, error) {
	var ret []int64
	var encodeType EncodeType
	var elemTotalCount uint32
	reader := bytes.NewReader(input)
	err := binary.Read(reader, binary.BigEndian, &encodeType)
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, binary.BigEndian, &elemTotalCount)
	if err != nil {
		return nil, err
	}
	if encodeType == EncodeTypeRawInt64 {
		for err == nil {
			var intNum int64
			err = binary.Read(reader, binary.BigEndian, &intNum)
			ret = append(ret, intNum)
		}
		if err == io.EOF {
			return ret, nil
		}
		return nil, err
	}
	var prevValue int64
	for {
		var uIntNum uint64
		err = binary.Read(reader, binary.BigEndian, &uIntNum)
		if err != nil {
			break
		}
		selector := int(uIntNum & ((uint64(1) << 4) - 1))
		bit := bitPerInteger[selector]
		elemCount := selectorToElements[selector]
		for i := 0; i < elemCount; i++ {
			var zigzagValue uint64 = 0
			if selector > 1 {
				zigzagValue = (uIntNum >> (4 + bit*i)) & ((uint64(1) << bit) - 1)
			}
			// ZigZagDecode
			diff := int64((zigzagValue >> 1) ^ -(zigzagValue & 1))
			currValue := diff + prevValue
			ret = append(ret, currValue)
			if len(ret) == int(elemTotalCount) {
				break
			}
			prevValue = currValue
		}
	}
	if err == io.EOF {
		return ret, nil
	}
	return nil, err
}

func encodeFloat64ToBuffer(buf *bytes.Buffer, v uint64, flag uint8) {
	nBytes := int((flag & 7) + 1)
	nShift := (64 - nBytes*8) * (int(flag) >> 3)
	v >>= nShift

	for i := 0; i < nBytes; i++ {
		_ = binary.Write(buf, binary.BigEndian, byte(v&255))
		v >>= 8
	}
}

func decodeFloat64Value(reader io.Reader, flag uint8) uint64 {
	var diff uint64 = 0
	nBytes := int((flag & 7) + 1)
	for i := 0; i < nBytes; i++ {
		var b byte
		_ = binary.Read(reader, binary.BigEndian, &b)
		diff = diff | ((uint64(b) & 255) << (8 * uint64(i)))
	}
	shiftWidth := (64 - nBytes*8) * (int(flag) >> 3)
	diff <<= shiftWidth
	return diff
}

func CompressFloat64(input []float64) []byte {
	nElements := len(input)
	byteLimit := nElements*8 + 1
	buf := bytes.NewBuffer(nil)
	var prevValue, prevDiff uint64 = 0, 0
	var prevFlag uint8 = 0

	_ = binary.Write(buf, binary.BigEndian, EncodeTypeXor)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(input)))

	//
	//  // Main loop
	for i := 0; i < nElements; i++ {
		currBits := math.Float64bits(input[i])
		diff := currBits ^ prevValue

		leadingZeros, trailingZeros := bits.LeadingZeros64(diff), bits.TrailingZeros64(diff)
		var nBytes, flag uint8

		if trailingZeros > leadingZeros {
			nBytes = uint8(8 - trailingZeros/8)
			if nBytes > 0 {
				nBytes -= 1
			}
			flag = 8 | nBytes
		} else {
			nBytes = uint8(8 - leadingZeros/8)
			if nBytes > 0 {
				nBytes -= 1
			}
			flag = nBytes
		}
		if i%2 == 0 {
			prevDiff, prevFlag = diff, flag
		} else {
			nBytes1, nBytes2 := (prevFlag&7)+1, (flag&7)+1
			if buf.Len()+int(nBytes1+nBytes2) <= byteLimit {
				_ = binary.Write(buf, binary.BigEndian, prevFlag|(flag<<4))
				encodeFloat64ToBuffer(buf, prevDiff, prevFlag)
				encodeFloat64ToBuffer(buf, diff, flag)
			} else {
				goto CopyAndExit
			}
		}
		prevValue = currBits
	}

	if nElements%2 != 0 {
		nBytes1, nBytes2 := (prevFlag&7)+1, 1
		if buf.Len()+int(nBytes1)+nBytes2 <= byteLimit {
			_ = binary.Write(buf, binary.BigEndian, prevFlag)
			encodeFloat64ToBuffer(buf, prevDiff, prevFlag)
			encodeFloat64ToBuffer(buf, 0, 0)
		} else {
			goto CopyAndExit
		}
	}
	return buf.Bytes()

CopyAndExit:
	buf = bytes.NewBuffer(nil)
	_ = binary.Write(buf, binary.BigEndian, EncodeTypeRawFloat64)
	for _, a := range input {
		_ = binary.Write(buf, binary.BigEndian, a)
	}
	return buf.Bytes()
}

func DecompressFloat64(input []byte) ([]float64, error) {
	var ret []float64
	var encodeType EncodeType
	var elemTotalCount uint32
	reader := bytes.NewReader(input)
	err := binary.Read(reader, binary.BigEndian, &encodeType)
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, binary.BigEndian, &elemTotalCount)
	if err != nil {
		return nil, err
	}
	if encodeType == EncodeTypeRawFloat64 {
		for err == nil {
			var float64Num float64
			err = binary.Read(reader, binary.BigEndian, &float64Num)
			ret = append(ret, float64Num)
		}
		if err == io.EOF {
			return ret, nil
		}
		return nil, err
	}
	var flags uint8
	var prevValue uint64

	for i := 0; i < int(elemTotalCount); i++ {
		if i%2 == 0 {
			_ = binary.Read(reader, binary.BigEndian, &flags)
		}

		flag := flags & 15
		flags >>= 4

		diff := decodeFloat64Value(reader, flag)
		prevValue = prevValue ^ diff
		ret = append(ret, math.Float64frombits(prevValue))
	}
	return ret, nil
}
