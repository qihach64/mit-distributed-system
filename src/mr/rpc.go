package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetTaskRequest struct {
	WorkerID string
}

type GetTaskResponse struct {
	Task Task
}

type UpdateTaskRequest struct {
	WorkerID string
	Task     Task
}

type UpdateTaskResponse struct {
}

type MarkTaskAsDoneRequest struct {
	Task     Task
	WorkerID string
}

type MarkTaskAsDoneResponse struct {
}

type RpcConnectionError struct {
	RPCName string
	Args    interface{}
	Err     error
}

func (e *RpcConnectionError) Error() string {
	return fmt.Sprintf("RPC call %s with args %+v failed with connection error: %v", e.RPCName, e.Args, e.Err)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
