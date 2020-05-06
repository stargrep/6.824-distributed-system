package mr

import (
	"fmt"
	"log"
	"net"
)
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	nMapper       int // starts with the mapper number, when down to zero than all completed.
	nReducer      int
	inputFiles    []string
	reduceResults []string
	finished	  bool
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) MapperInfo(args *EmptyArgs, reply *MapperInfoReply) error {
	reply.Files = m.inputFiles
	reply.NMapper = m.nMapper
	reply.NReducer = m.nReducer
	return nil
}

func (m *Master) ResultHandler(args *SendResArgs, reply *EmptyReply) error {
	result := args.Result
	fmt.Println("send here", len(result))
	oname := "mr-out-1-1"
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(result) {
		fmt.Fprintf(ofile, "%v %v\n", result[i].Key, result[i].Value)
		i++
	}

	ofile.Close()
	m.finished = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.finished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nMapper:       len(files),
		nReducer:      nReduce,
		inputFiles:    files,
		reduceResults: make([]string, 0),
		finished:	   false,
	}
	//fmt.Println(m)

	m.server()
	return &m
}

//func fileHelper(files []string) {
//	filepath.Dir(files[0])
//	filepath.Abs(files[0])
//	dir, err := filepath.Abs(files[0])
//	fmt.Print(dir, err)
//
//	file, err := os.Open(files[1])
//	if err != nil {
//		log.Fatalf("cannot open %v", files[0])
//	}
//	//content, err := ioutil.ReadAll(file)
//	//if err != nil {
//	//	log.Fatalf("cannot read %v", files[0])
//	//}
//	fmt.Println(file.Name())
//	file.Close()
//}
