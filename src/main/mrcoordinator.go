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
	"log"
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
		debugInfo(m)
	}

	time.Sleep(time.Second)
}

func debugInfo(m *mr.Coordinator) {
	log.Println("🤖=====================================")
	printMapTasks(m)
	printReduceTasks(m)
	printWorkerAssignments(m)
}

func printMapTasks(m *mr.Coordinator) {
	log.Println("-------------[map task]--------------")
	for _, task := range m.MapTasks {
		log.Printf("%+v\n", task)
	}
}

func printReduceTasks(m *mr.Coordinator) {
	log.Println("-------------[reduce task]--------------")
	for _, task := range m.ReduceTasks {
		log.Printf("%+v\n", task)
	}
}

func printWorkerAssignments(m *mr.Coordinator) {
	log.Println("-------------[worker]--------------")
	for i, task := range m.Workers {
		log.Printf("worker_id=%s: Task=%+v\n", i, task)
	}
}
