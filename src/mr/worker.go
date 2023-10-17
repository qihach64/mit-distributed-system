package mr

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func (w *Worker) Run() error {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	for {
		// 1. ask the coordinator for a task
		task, err := w.GetTask()
		if err != nil {
			return err
		} // 2. execute the task
		if task == nil {
			log.Printf("No task to execute, worker %s is waiting ... \n", w.ID)
			time.Sleep(time.Second)
			continue
		}
		if task.GetType() == MAP {
			mapTask := task.(MapTask)
			log.Printf("Worker %s is executing map task %+v\n", w.ID, mapTask)
			if err := w.DoMapTask(&mapTask); err != nil {
				return err
			}
		} else {
			reduceTask := task.(ReduceTask)
			log.Printf("Worker %s is executing reduce task %+v\n", w.ID, reduceTask)
			if err := w.DoReduceTask(&reduceTask); err != nil {
				return err
			}
		}
		// 3. update the coordinator marking the task status as Done
		w.MarkTaskAsDone(task)
	}
}

func (w *Worker) GetTask() (Task, error) {
	request := GetTaskRequest{WorkerID: w.ID}
	response := GetTaskResponse{}
	if err := call("Coordinator.GetTask", &request, &response); err != nil {
		return nil, err
	}
	return response.Task, nil
}

func (w *Worker) DoMapTask(mapTask *MapTask) error {
	filename := mapTask.InputFile
	content, err := readFileContent(filename)
	if err != nil {
		return err
	}
	kvs := w.MapFunc(filename, content)

	fileMap := make(map[int]*os.File)
	defer func() {
		for _, file := range fileMap {
			file.Close()
		}
	}()

	for _, kv := range kvs {
		reduceID := ihash(kv.Key) % mapTask.ReduceNum
		immediateFileName := fmt.Sprintf("mr-%d-%d", mapTask.ID, reduceID)
		if _, exist := fileMap[reduceID]; !exist {
			ofile, err := os.OpenFile(immediateFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				return err
			}
			fileMap[reduceID] = ofile
			mapTask.ImmediateFiles[reduceID] = immediateFileName
		}
		fmt.Fprintf(fileMap[reduceID], "%v %v\n", kv.Key, kv.Value)
	}
	return nil
}

func (w *Worker) MarkTaskAsDone(task Task) error {
	request := MarkTaskAsDoneRequest{WorkerID: w.ID, Task: task}
	response := MarkTaskAsDoneResponse{}
	if err := call("Coordinator.MarkTaskAsDone", &request, &response); err != nil {
		return err
	}
	return nil
}

func (w *Worker) DoReduceTask(reduceTask *ReduceTask) error {
	var intermediate []KeyValue
	for immediateFileName := range reduceTask.ImmediateFiles {
		kvs, err := readIntermediateFile(immediateFileName)
		if err != nil {
			return err
		}
		intermediate = append(intermediate, kvs...)
	}

	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	oname := fmt.Sprintf("mr-out-%d", reduceTask.ID)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.ReduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return nil
}

func readIntermediateFile(immediateFile string) ([]KeyValue, error) {
	f, err := os.Open(immediateFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var kvs []KeyValue
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid intermediate file format: %s", line)
		}
		kvs = append(kvs, KeyValue{Key: fields[0], Value: fields[1]})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return kvs, nil
}

func readFileContent(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	content, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	f.Close()
	return string(content), nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Fatalf("call %s failed with error: %v\n%+v", rpcname, err, err)
	}
	return err
}
