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
	printMapTasks(m)
	printReduceTasks(m)
	for m.Done() == false {
		time.Sleep(time.Second)
		fmt.Printf("Coordinator: waiting for tasks to execute...\n")
	}

	time.Sleep(time.Second)
}

func printMapTasks(m *mr.Coordinator) {
	for i, task := range m.MapTasks {
		fmt.Printf("Task %d: file=%s, status=%v\n", i, task.File, task.Status)
	}
}

func printReduceTasks(m *mr.Coordinator) {
	for i, task := range m.ReduceTasks {
		fmt.Printf("Task %d: id=%d, status=%v\n", i, task.ReduceID, task.Status)
		for file, status := range task.ImmediateFileStatus {
			fmt.Printf("Immediate files:  %s: %v\n", file, status)
		}
	}
}
