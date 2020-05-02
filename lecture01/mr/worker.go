package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "../commons"

// for sorting by key.
type ByKey []commons.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string, nReducer int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) % nReducer
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []commons.KeyValue) {

	// Your worker implementation here.
	info := CallMapperInfo()
	reducerChans := getNReducerChan(info.NReducer)
	completeChan := make(chan []commons.KeyValue)

	var reducerGroup sync.WaitGroup
	var mergeGroup sync.WaitGroup
	mergeGroup.Add(1)
	go merger(completeChan, &mergeGroup)

	for i := 0; i < len(reducerChans); i++ {
		reducerGroup.Add(1)
		go reducer(reducerChans[i], completeChan, &reducerGroup)
	}

	var mapperGroup sync.WaitGroup
	for _, file := range info.Files {
		mapperGroup.Add(1)
		go mapper(mapf, file, reducerChans, &mapperGroup)
	}

	mapperGroup.Wait()
	for i := 0; i < len(reducerChans); i++ {
		close(reducerChans[i])
	}
	reducerGroup.Wait()
	close(completeChan)
	mergeGroup.Wait()
}

func getNReducerChan(nReducer int) []chan commons.KeyValue {
	var arr []chan commons.KeyValue
	for i := 0; i < nReducer; i++ {
		arr = append(arr, make(chan commons.KeyValue))
	}
	return arr
}

func merger(completeChan chan []commons.KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()
	kvs := make([] commons.KeyValue, 0)

	for {
		kv, more := <-completeChan
		kvs = append(kvs, kv...)
		if !more {
			sort.Sort(ByKey(kvs)) // can improve to k-way merge
			SendResToMaster(kvs)
			return
		}
	}
}

func mapper(mapf func(string, string) []commons.KeyValue,
			fileName string,
			reducerChan []chan commons.KeyValue,
			wg *sync.WaitGroup) {
	defer wg.Done()

	nReducer := len(reducerChan)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	check(err)
	kvs := mapf("", string(content))

	for _, kv := range kvs {
		reducerChan[ihash(kv.Key, nReducer)] <- kv
	}

}

func reducer(reducerChan chan commons.KeyValue,
			completeChan chan []commons.KeyValue,
			wg *sync.WaitGroup) {
	defer wg.Done()

	kvs := make([] commons.KeyValue, 0)

	for {
		kv, more := <-reducerChan
		kvs = append(kvs, kv)
		if !more {
			//fmt.Println("reducers", len(kvs))
			sort.Sort(ByKey(kvs))

			res := make([] commons.KeyValue, 0)
			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				if len(kvs[i].Key) > 0 {
					res = append(res, commons.KeyValue{Key: kvs[i].Key, Value: strconv.Itoa(j - i)})
				}
				i = j
			}
			//fmt.Println("reducers:- ", len(res))
			completeChan <- res
			return
		}
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func CallMapperInfo() MapperInfoReply {
	res := MapperInfoReply{}
	call("Master.MapperInfo", &EmptyArgs{}, &res)
	return res
}

func SendResToMaster(kvs []commons.KeyValue) {
	call("Master.ResultHandler", &SendResArgs{Result: kvs}, &EmptyReply{})
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
