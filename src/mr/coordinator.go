package mr

import (
	"encoding/gob"
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

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

func (s TaskType) String() string {
	return [...]string{"MAP", "REDUCE"}[s]
}

type Task interface {
	GetType() TaskType
	GetStatus() TaskStatus
}

type MapTask struct {
	ID        int
	Status    TaskStatus
	InputFile string
	ReduceNum int
}

func (m MapTask) GetType() TaskType {
	return MAP
}

func (m MapTask) GetStatus() TaskStatus {
	return m.Status
}

type ReduceTask struct {
	ID             int
	Status         TaskStatus      // The status of the reduce task
	ImmediateFiles map[string]bool // The immediate files that are generated by the map tasks
}

func (r ReduceTask) GetType() TaskType {
	return REDUCE
}

func (r ReduceTask) GetStatus() TaskStatus {
	return r.Status
}

type Coordinator struct {
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	// The key is the worker id, the value is the task assigned to the worker
	// if the task is nil, it means the worker is idle
	Workers map[string]Task
}

func (c *Coordinator) GetTask(
	request *GetTaskRequest, response *GetTaskResponse) error {
	fmt.Printf("GetTask request: %v\n", request)
	for mapID, m := range c.MapTasks {
		if m.Status == TODO {
			c.MapTasks[mapID].Status = IN_PROGRESS
			c.MapTasks[mapID].ReduceNum = len(c.ReduceTasks)
			c.Workers[request.WorkerID] = c.MapTasks[mapID]
			response.Task = c.MapTasks[mapID]
			return nil
		}
	}
	for reduceID, r := range c.ReduceTasks {
		if r.Status == TODO {
			c.ReduceTasks[reduceID].Status = IN_PROGRESS
			c.Workers[request.WorkerID] = c.ReduceTasks[reduceID]
			response.Task = c.ReduceTasks[reduceID]
			return nil
		}
	}
	return nil
}

func (c *Coordinator) MarkTaskAsDone(request *MarkTaskAsDoneRequest, response *MarkTaskAsDoneResponse) error {
	fmt.Printf("MarkTaskAsDone request: %v\n", request)
	if request.Task.GetType() == MAP {
		mapTask := request.Task.(MapTask)
		if mapTask.ID < 0 || mapTask.ID >= len(c.MapTasks) {
			return fmt.Errorf("invalid map task id: %d", mapTask.ID)
		}
		c.MapTasks[mapTask.ID].Status = DONE
	} else {
		reduceTask := request.Task.(ReduceTask)
		c.ReduceTasks[reduceTask.ID] = reduceTask
		if reduceTask.ID < 0 || reduceTask.ID >= len(c.ReduceTasks) {
			return fmt.Errorf("invalid reduce task id: %d", reduceTask.ID)
		}
		c.ReduceTasks[reduceTask.ID].Status = DONE
	}
	c.Workers[request.WorkerID] = nil
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
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
	workers := make(map[string]Task)
	c := Coordinator{MapTasks: mapTasks, ReduceTasks: reduceTasks, Workers: workers}
	c.server()
	return &c
}

func initMapTasks(files []string) []MapTask {
	m := make([]MapTask, len(files))
	for i, fileName := range files {
		m[i] = MapTask{ID: i, InputFile: fileName, Status: TODO}
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
