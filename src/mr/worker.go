package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Worker struct {
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
}

// main/mrworker.go calls this function.
func CreateWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *Worker {
	return &Worker{MapFunc: mapf, ReduceFunc: reducef}
}

func (w *Worker) Run() {
	for {
		// 1. ask the coordinator for a task
		assignment, hasMoreTasks := w.GetAssignment()
		if !hasMoreTasks {
			break
		}

		// 2. execute the task
		if assignment.Status == MAP {
			w.DoMapTask(assignment)
		} else if assignment.Status == REDUCE {
			w.DoReduceTask(assignment)
		} else {
			log.Fatalf("Unexpected worker assignment status: %v\n", assignment.Status)
		}

		// 3. report the result to the coordinator
		w.NotifyCompletion(assignment)
	}
}

func (w *Worker) GetAssignment() (*WorkerAssignment, bool) {
	fmt.Printf("Worker: got assignment hasMoreTask=%v\n", false)
	return nil, false
}

func (w *Worker) DoMapTask(assignment *WorkerAssignment) {
	// TODO: implement DoMapTask
}

func (w *Worker) DoReduceTask(assignment *WorkerAssignment) {
	// TODO: implement DoReduceTask
}

func (w *Worker) NotifyCompletion(assignment *WorkerAssignment) {
	// TODO: implement NotifyCompletion
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}
