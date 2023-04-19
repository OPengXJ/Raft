package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"sort"
	"strconv"
	"time"
)

// sort
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		CallHandOutTask(mapf, reducef)
		time.Sleep(1 * time.Second)
	}
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

func CallHandOutTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := HandOutTaskReply{}
	args := HandOutTaskArgs{}
	call("Coordinator.HandOutTask", &args, &reply)
	if reply.Y.State == 0 {
		time.Sleep(1 * time.Second)
		return
	}
	// the task is map
	if reply.Y.TaskType == 1 {
		fileNames := HandleMap(reply.Y, mapf)
		taskDoneArgs := TaskDoneArgs{
			TaskName:  reply.Y.Taskname,
			TaskType:  1,
			FileNames: fileNames,
		}
		taskDoneReply := TaskDoneReply{}
		call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	} else {
		HandleReduce(reply.Y, reducef)
		taskDoneArgs := TaskDoneArgs{
			TaskName: reply.Y.Taskname,
			TaskType: 2,
		}
		taskDoneReply := TaskDoneReply{}
		call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
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
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func HandleMap(task Task, mapf func(string, string) []KeyValue) []string {

	intermediate := []KeyValue{}
	filename := task.InputFileName
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// output intermediate to R local file
	R := task.NReduce
	fileNames := make([]string, R)
	tempFileNames := make([]string, R)
	files := make([]*os.File, R)
	for i := 0; i < R; i++ {
		filename := "mr" + "-" + strconv.Itoa(task.Taskname) + "-" + strconv.Itoa(i)
		// file, _ := os.Create(filename)
		tempFile, _ := ioutil.TempFile("./", "temp"+filename)

		defer tempFile.Close()

		tempname := tempFile.Name()
		defer os.Remove(tempname)

		files[i] = tempFile
		fileNames[i] = filename
		tempFileNames[i] = tempname
	}

	for _, kv := range intermediate {
		// make a key or some same key to same file
		index := ihash(kv.Key) % R
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}
	for i, filename := range tempFileNames {
		os.Rename(filename, "./"+fileNames[i])
	}
	return fileNames
}

func HandleReduce(task Task, reducef func(string, []string) string) {
	//read intermediate keyvalue
	intermediate := []KeyValue{}
	for _, v := range task.IntermediateFileNames {
		file, _ := os.Open(v)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// sort first	for preference
	sort.Sort(ByKey(intermediate))

	fileName := "mr-out-" + strconv.Itoa(task.Taskname)
	// ofile, _ := os.Create(fileName)
	tempFile, _ := ioutil.TempFile("./", fileName)
	defer tempFile.Close()
	tempname := tempFile.Name()
	defer os.Remove(tempname)

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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(tempname, "./"+fileName)
}
