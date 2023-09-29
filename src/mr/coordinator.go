package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus int

const (
	TODO TaskStatus = iota
	IN_PROGRESS
	DONE
)

func (s TaskStatus) String() string {
	return [...]string{"TODO", "IN_PROGRESS", "DONE"}[s]
}

type WorkerStatus int

const (
	IDEL WorkerStatus = iota
	MAP
	REDUCE
	DEAD
)

func (s WorkerStatus) String() string {
	return [...]string{"IDEL", "MAP", "REDUCE", "DEAD"}[s]
}

type MapTask struct {
	Status    TaskStatus
	InputFile string
}

type ReduceTask struct {
	Status         TaskStatus      // The status of the reduce task
	ImmediateFiles map[string]bool // The immediate files that are generated by the map tasks
}

type WorkerAssignment struct {
	Status       WorkerStatus
	MapTaskID    int // -1 if no map task is assigned
	ReduceTaskID int // -1 if no reduce task is assigned
}

type Coordinator struct {
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	Workers     map[int]WorkerAssignment
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	fmt.Printf("sockname: %s\n", sockname)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, r := range c.ReduceTasks {
		if r.Status != DONE {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	mapTasks := initMapTasks(files)
	reduceTasks := initReduceTasks(nReduce)
	workers := make(map[int]WorkerAssignment)
	c := Coordinator{MapTasks: mapTasks, ReduceTasks: reduceTasks, Workers: workers}
	c.server()
	return &c
}

func initMapTasks(files []string) []MapTask {
	m := make([]MapTask, len(files))
	for i, fileName := range files {
		m[i] = MapTask{InputFile: fileName, Status: TODO}
	}
	return m
}

func initReduceTasks(nReduce int) []ReduceTask {
	r := make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		r[i] = ReduceTask{Status: TODO, ImmediateFiles: make(map[string]bool)}
	}
	return r
}
