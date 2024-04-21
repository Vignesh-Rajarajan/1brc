package attempts

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type MMapRead struct {
	input  string
	result StationMap
}

func NewMMapRead(input string) *MMapRead {
	return &MMapRead{
		input:  input,
		result: make(StationMap),
	}
}

func (m *MMapRead) ExecuteBillionRow() error {

	f, err := os.Open(m.input)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}

	size := fi.Size()

	// check if file size is too large to be represented as int
	if size <= 0 || size != int64(int(size)) {
		return fmt.Errorf("invalid file size")
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer func() {
		if err := syscall.Munmap(data); err != nil {
			log.Fatalf("error while unmapping memory %v", err)
		}
	}()

	resultStream := make(chan StationMap, 10)
	chunkStream := make(chan []byte, 15)
	var wg sync.WaitGroup

	for i := 0; i < NWorkers; i++ {
		wg.Add(1)
		go func() {
			for ch := range chunkStream {
				processReadChunk(ch, resultStream)
			}
			wg.Done()
		}()
	}

	go func() {
		buf := make([]byte, ReadBytes)
		leftOver := make([]byte, 0, ReadBytes)
		var i int

		for {

			if i >= len(data) || len(data) == 0 {
				break
			}

			limit := i + ReadBytes
			if len(data[i:]) < ReadBytes {
				limit = len(data) - 1
			}
			buf = data[i:limit]
			i += ReadBytes

			toSend := make([]byte, 0, ReadBytes)
			copy(toSend, buf)

			lastNewLineIdx := bytes.LastIndex(buf, []byte("\n"))

			toSend = append(leftOver, buf[:lastNewLineIdx+1]...)
			leftOver = make([]byte, len(buf[lastNewLineIdx+1:]))
			copy(leftOver, buf[lastNewLineIdx+1:])
			chunkStream <- toSend
		}
		close(chunkStream)
		wg.Wait()
		close(resultStream)
	}()

	for res := range resultStream {
		for cityId, station := range res {
			if val, ok := m.result[cityId]; ok {
				val.Count += station.Count
				val.Sum += station.Sum
				if station.Max > val.Max {
					val.Max = station.Max
				}
				if station.Min < val.Min {
					val.Min = station.Min
				}
			} else {
				m.result[cityId] = station
			}
		}
	}

	m.printResult()
	return nil

}

func (m *MMapRead) printResult() {
	result := make([]StationData, len(m.result))
	keys := make([]string, 0, len(m.result))
	count := 0

	for _, val := range m.result {
		keys = append(keys, val.StationName)
		result[count] = StationData{
			StationName: val.StationName,
			Count:       val.Count,
			Min:         val.Min,
			Max:         val.Max,
			Sum:         val.Sum,
		}
		count++
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StationName < result[j].StationName
	})

	var strBuilder strings.Builder

	for _, key := range result {
		strBuilder.WriteString(fmt.Sprintf("%s Min: %.1f Max: %.1f Avg: %.1f \n", key.StationName, float64(key.Min/10), key.Max, float64(key.Sum/key.Count)))
	}
	fmt.Println(strBuilder.String()[:strBuilder.Len()-2])
}

func processReadChunk(buff []byte, stream chan<- StationMap) {
	toSend := make(StationMap)

	var start int
	var city string

	stringBuf := string(buff)

	for i, char := range stringBuf {
		switch char {
		case ';':
			city = stringBuf[start:i]
			start = i + 1
		case '\n':
			if (i-start) > 1 && len(city) != 0 {
				temperature := customStringToIntParser(stringBuf[start:i])
				start = i + 1
				hash := customHash([]byte(city))
				if val, ok := toSend[hash]; ok {
					val.Count++
					val.Sum += temperature
					if (temperature) > val.Max {
						val.Max = temperature
					}
					if (temperature) < val.Min {
						val.Min = temperature
					}

				} else {
					toSend[hash] = &StationData{
						StationName: city,
						Count:       1,
						Sum:         int(temperature),
						Min:         (temperature),
						Max:         (temperature),
					}
				}
				city = ""
			}
		}
	}
	stream <- toSend
}

func customStringToIntParser(s string) int {
	var isNegative bool
	if s[0] == '-' {
		isNegative = true
		s = s[1:]
	}
	var result int

	for _, v := range s {
		if v == '.' {
			continue
		}
		result = result*10 + int(v-48)
	}

	if isNegative {
		return -result
	}
	return result
}

func roundToTwoDecimalPlaces(f float64) float64 {
	rounded := math.Round(f * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}
