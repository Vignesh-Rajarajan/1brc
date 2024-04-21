package attempts

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

type NativeRead struct {
	input  string
	result StationMap
}

func NewNativeRead(input string) *NativeRead {
	return &NativeRead{
		input:  input,
		result: make(StationMap),
	}
}

func bytesToInteger(b []byte) int {
	var result int
	isNegative := false

	for _, v := range b {
		if v == 45 {
			isNegative = true
			continue
		}
		result = result*10 + int(v-48)
	}
	if isNegative {
		result = -result
	}
	return result
}

func parseLine(readingIndex int, line, name, temperature []byte) (nextReadingIndex, nameLen, temperatureLen int) {

	i, j := readingIndex, 0

	for line[i] != ';' {
		name[j] = line[i]
		i++
		j++
	}
	i++
	k := 0
	for i < len(line) && line[i] != '\n' {
		if line[i] == '.' {
			i++
			continue
		}
		temperature[k] = line[i]
		i++
		k++
	}
	readingIndex = i + 1
	return readingIndex, j, k

}

func consumer(input chan []byte, output chan StationMap, wg *sync.WaitGroup) {
	defer wg.Done()
	data := make(StationMap)
	nameBuffer := make([]byte, 100)
	temperatureBuffer := make([]byte, 100)
	for line := range input {
		readingIndex := 0
		for readingIndex < len(line) {
			next, nameSize, temperatureSize := parseLine(readingIndex, line, nameBuffer, temperatureBuffer)
			readingIndex = next
			name := nameBuffer[:nameSize]
			temperature := bytesToInteger(temperatureBuffer[:temperatureSize])
			id := customHash(name)
			station, ok := data[id]
			if !ok {
				station = &StationData{StationName: string(name), Min: temperature, Max: temperature, Sum: temperature, Count: 1}
				data[id] = station
			} else {
				if temperature < station.Min {
					station.Min = temperature
				}
				if temperature > station.Max {
					station.Max = temperature
				}
				station.Sum += temperature
				station.Count++
			}
			data[id] = station

		}
	}
	output <- data
}
func (r *NativeRead) ExecuteBillionRow() error {
	inputChannels := make([]chan []byte, NWorkers)
	outputChannels := make([]chan StationMap, NWorkers)

	var wg sync.WaitGroup
	wg.Add(NWorkers)

	for i := 0; i < NWorkers; i++ {
		input := make(chan []byte, ChanelBuffer)
		output := make(chan StationMap, 1)
		go consumer(input, output, &wg)
		inputChannels[i] = input
		outputChannels[i] = output
	}
	file, err := os.Open(r.input)
	if err != nil {
		return err
	}
	defer file.Close()

	readBuffer := make([]byte, ReadBytes)
	leftoverBuff := make([]byte, ReadBytes)
	leftoverSize := 0
	currentWorker := 0
	for {

		n, err := file.Read(readBuffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		m := 0
		for i := n - 1; i >= 0; i-- {
			if readBuffer[i] == '\n' {
				m = i
				break
			}
		}

		data := make([]byte, m+leftoverSize)
		copy(data, leftoverBuff[:leftoverSize])
		copy(data[leftoverSize:], readBuffer[:m])
		copy(leftoverBuff, readBuffer[m+1:n])
		leftoverSize = n - m - 1

		inputChannels[currentWorker] <- data
		currentWorker = (currentWorker + 1) % NWorkers
	}
	for i := 0; i < NWorkers; i++ {
		close(inputChannels[i])
	}
	wg.Wait()
	for i := 0; i < NWorkers; i++ {
		close(outputChannels[i])
	}

	aggregateData := make(StationMap)
	for i := 0; i < NWorkers; i++ {
		for k, stationData := range <-outputChannels[i] {
			station, ok := aggregateData[k]
			if !ok {
				aggregateData[k] = stationData
			} else {
				if stationData.Min < station.Min {
					station.Min = stationData.Min
				}
				if stationData.Max > station.Max {
					station.Max = stationData.Max
				}
				station.Sum += stationData.Sum
				station.Count += stationData.Count
			}
		}
	}
	r.result = aggregateData
	r.printResult()
	return nil
}

func (r *NativeRead) printResult() {
	result := make(map[string]*StationData, len(r.result))
	keys := make([]string, 0, len(r.result))

	for _, val := range r.result {
		keys = append(keys, val.StationName)
		result[val.StationName] = val
	}
	sort.Strings(keys)

	fmt.Print("{")
	for _, k := range keys {
		val := result[k]
		fmt.Printf("%s=%.1f/%.1f/%.1f \n", k, float64(val.Min/10), float64(val.Sum/10)/float64(val.Count), float64(val.Max/10))
	}
	fmt.Print("}")
	fmt.Println()
}
