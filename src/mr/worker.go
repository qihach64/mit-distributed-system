package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"

	"github.com/google/uuid"
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
	ID         string //UUID for the worker
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
}

func CreateWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) *Worker {
	// When generating a unique identifier for the worker, we use a UUID (Universally Unique Identifier)
	// which is a 128-bit number that is guaranteed to be unique across time and space.
	// UUIDs are 36-character alphanumeric strings.
	// They are written in 5 groups of hexadecimal digits separated by hyphens[3]. The length of each group is: 8-4-4-4-12[3].
	// For example, a UUID could look like this: acde070d-8c4c-4f0d-9d8a-162843c10333
	id := uuid.New().String()
	return &Worker{ID: id, MapFunc: mapf, ReduceFunc: reducef}
}

func (w *Worker) Run() {
	// for {
	// 1. ask the coordinator for a task
	assignment := w.GetTaskAssignment()
	if assignment == nil {
		return
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
	// }
}

func (w *Worker) GetTaskAssignment() *WorkerAssignment {
	request := GetTaskAssignmentRequest{WorkerID: w.ID}
	response := GetTaskAssignmentResponse{}
	ok := call("Coordinator.GetTaskAssignment", &request, &response)
	if !ok {
		fmt.Printf("GetTaskAssignment failed!\n")
		return nil
	}
	fmt.Printf("GetTaskAssignment response: %v\n", response)
	return response.Task
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
