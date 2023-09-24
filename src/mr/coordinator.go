package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Status int

const (
	TODO Status = iota
	IN_PROGRESS
	DONE
)

func (s Status) String() string {
	return [...]string{"TODO", "IN_PROGRESS", "DONE"}[s]
}

type MapTaskStatus struct {
	Status Status
	File   string
}

type ReduceTaskStatus struct {
	Status              Status
	ImmediateFileStatus map[string]Status
	ReduceID            int
}

type Coordinator struct {
	// Your definitions here.
	MapTasks    []MapTaskStatus
	ReduceTasks []ReduceTaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for _, r := range c.ReduceTasks {
		if r.Status != DONE {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := initMapTasks(files)
	reduceTasks := initReduceTasks(nReduce)
	c := Coordinator{MapTasks: mapTasks, ReduceTasks: reduceTasks}

	// Your code here.

	// c.server()
	return &c
}

func initMapTasks(files []string) []MapTaskStatus {
	m := make([]MapTaskStatus, len(files))
	for i, fileName := range files {
		m[i] = MapTaskStatus{File: fileName, Status: TODO}
	}
	return m
}

func initReduceTasks(nReduce int) []ReduceTaskStatus {
	r := make([]ReduceTaskStatus, nReduce)
	for i := 0; i < nReduce; i++ {
		r[i] = ReduceTaskStatus{ReduceID: i, Status: TODO, ImmediateFileStatus: make(map[string]Status)}
	}
	return r
}
