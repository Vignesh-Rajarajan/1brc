package attempts

type OneBillionRowExecution interface {
	ExecuteBillionRow() error
}

type StationMap map[uint64]*StationData

var NWorkers = 75
var ChanelBuffer = 75
var ReadBytes = 2048 * 2048

type StationData struct {
	StationName string
	Min         int
	Max         int
	Sum         int
	Count       int
}

func customHash(s []byte) uint64 {
	var h uint64 = 5831
	for _, c := range s {
		h += h*31 + uint64(c)
	}
	return h
}
