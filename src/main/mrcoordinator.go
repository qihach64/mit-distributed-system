package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.5840/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	debugInfo(m)
	for m.Done() == false {
		time.Sleep(time.Second)
		fmt.Printf("Coordinator: waiting for tasks to execute...\n")
		debugInfo(m)
	}

	time.Sleep(time.Second)
}

func debugInfo(m *mr.Coordinator) {
	printMapTasks(m)
	printReduceTasks(m)
	printWorkerAssignments(m)
}

func printMapTasks(m *mr.Coordinator) {
	for i, task := range m.MapTasks {
		fmt.Printf("map_id=%d: status=%s, input_file=%s\n", i, task.Status, task.InputFile)
	}
}

func printReduceTasks(m *mr.Coordinator) {
	for i, task := range m.ReduceTasks {
		fmt.Printf("reduce_id=%d: status=%s, files=%v\n", i, task.Status, task.ImmediateFiles)
	}
}

func printWorkerAssignments(m *mr.Coordinator) {
	for i, task := range m.Workers {
		fmt.Printf("worker_id=%s: Task=%v\n", i, task.GetType())
	}
}
