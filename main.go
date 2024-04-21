package main

import (
	"1brc/attempts"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"
)

var executionType = flag.String("executionType", "native_read", "execution type")
var input = flag.String("input", "/Users/vignesh/go/src/measurements.txt", "input file")

func main() {
	flag.Parse()

	var execution attempts.OneBillionRowExecution
	if *input == "" {
		fmt.Println("input cannot be empty")
		os.Exit(1)
	}

	f, err := os.Create(fmt.Sprintf("./profiles/%s-%s.prof", "execution", *executionType))
	if err != nil {
		fmt.Println("error while creating profile", err)
		os.Exit(1)

	}
	defer f.Close()
	trace.Start(f)
	defer trace.Stop()

	fcpu, err := os.Create(fmt.Sprintf("./profiles/%s-%s.prof", "cpu", *executionType))
	if err != nil {
		fmt.Println("error while creating cpu profile", err)
		os.Exit(1)
	}
	defer fcpu.Close()
	if err := pprof.StartCPUProfile(fcpu); err != nil {
		fmt.Println("error while starting cpu profile", err)
		os.Exit(1)

	}
	defer pprof.StopCPUProfile()
	started := time.Now()
	execution = attempts.NewNativeRead(*input)
	//execution = attempts.NewMMapRead(*input)

	if err := execution.ExecuteBillionRow(); err != nil {
		fmt.Println("error while executing billion row", err)
		os.Exit(1)
	}

	fmt.Printf("time taken %0.6f", time.Since(started).Seconds())

	fmem, err := os.Create(fmt.Sprintf("./profiles/%s-%s.prof", "memory", *executionType))
	if err != nil {
		fmt.Println("error while creating memory profile", err)
		os.Exit(1)
	}
	defer fmem.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(fmem); err != nil {
		fmt.Println("error while starting memory profile", err)
		os.Exit(1)
	}
}
