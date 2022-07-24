package main

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompressInt64SingleCase(t *testing.T) {
	var testData []int64
	for i := 0; i < 23; i++ {
		testData = append(testData, 0)
	}
	testData = append(testData, 10000000)
	compressed := CompressInt64(testData)
	ret, err := DeCompressInt64(compressed)
	assert.Nil(t, err)
	assert.Equal(t, testData, ret)
}

func TestCompressFloat64SingleCase(t *testing.T) {
	var testData []float64
	for i := 0; i < 23; i++ {
		testData = append(testData, 12.21212)
	}
	testData = append(testData, 10000000.123)
	compressed := CompressFloat64(testData)
	ret, err := DecompressFloat64(compressed)
	assert.Nil(t, err)
	assert.Equal(t, testData, ret)
}

func TestCompressInt64(t *testing.T) {
	caseNo := 0
	for _, caseCount := range []int{1000, 10000, 100000, 100000} {
		for _, baseNum := range []int64{1, 100, 10000, 1000000, 10000000} {
			for _, intRange := range []int64{1, 10, 1000, 100000, 1000000} {
				caseNo += 1
				testCompressInt64(t, caseNo, caseCount, baseNum, intRange)
			}
		}
	}
}

func TestCompressFloat64(t *testing.T) {
	caseNo := 0
	for _, caseCount := range []int{100, 10000, 100000, 5000000} {
		for _, baseNum := range []float64{1.12213, 100.242, 10000.23354, 1000000.098343, 10000000.23254} {
			for _, intRange := range []int64{1, 10, 1000, 100000, 1000000} {
				caseNo += 1
				testCompressFloat64(t, caseNo, caseCount, baseNum, intRange)
			}
		}
	}
}

func testCompressInt64(t *testing.T, caseNum, caseCount int, baseNum, intRange int64) {
	var testData []int64
	base := rand.Int63n(baseNum)
	for i := 0; i < caseCount; i++ {
		testData = append(testData, base+rand.Int63n(intRange))
	}
	start := time.Now()
	compressed := CompressInt64(testData)
	end1 := time.Now()
	ret, err := DeCompressInt64(compressed)
	t.Logf("case %d: num_count=%d base=%d range=%d compress_rate=%.4f compress_cost=%s decompress_cost=%s total_cost=%s",
		caseNum, caseCount, baseNum, intRange,
		float64(len(compressed))/float64(len(testData))/8, end1.Sub(start), time.Now().Sub(end1), time.Now().Sub(start))
	assert.Nil(t, err)
	assert.Equal(t, testData, ret)
}

func testCompressFloat64(t *testing.T, caseNum, caseCount int, baseNum float64, intRange int64) {
	var testData []float64
	for i := 0; i < caseCount; i++ {
		testData = append(testData, baseNum+float64(rand.Int63n(intRange)))
	}
	start := time.Now()
	compressed := CompressFloat64(testData)
	end1 := time.Now()
	ret, err := DecompressFloat64(compressed)
	t.Logf("case %d: num_count=%d base=%f range=%d compress_rate=%.4f compress_cost=%s decompress_cost=%s total_cost=%s",
		caseNum, caseCount, baseNum, intRange,
		float64(len(compressed))/float64(len(testData))/8, end1.Sub(start), time.Now().Sub(end1), time.Now().Sub(start))
	assert.Nil(t, err)
	assert.Equal(t, testData, ret)
}

func TestConvert(t *testing.T) {
	a := 1.0
	t.Log(int64(a))
}

func TestDecodeEncodeFloat64(t *testing.T) {
	v := 12.1212
	buf := bytes.NewBuffer(nil)
	a := math.Float64bits(v)
	encodeFloat64ToBuffer(buf, a, 7)
	b := decodeFloat64Value(buf, 7)
	fmt.Println(a, b)
}
