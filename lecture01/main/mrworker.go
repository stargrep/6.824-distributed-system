package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"../commons"
	"time"
)
import "../mr"
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	start := time.Now()

	//... do something

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf, _ := loadPluginNow(os.Args[1])
	//mr.Worker(mapf, reducef)
	mr.Worker(mapf)
	fmt.Println(time.Since(start))
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPluginNow(filename string) (func(string, string) []commons.KeyValue, func(string, []string) string) {
	//fmt.Println(filename)
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []commons.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
