package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	state               int // MASTER_INIT;MAP_FINISHED;REDUCE_FINISHED
	mapTask             []*Task
	reduceTask          map[int]*Task
	nMap                int // M
	nReduce             int //R
	mapTaskFinishNum    int
	reduceTaskFinishNum int
	mu                  sync.Mutex
}

type Task struct {
	TaskType              int // 1.map 2.reduce
	State                 int //TASK_INIT;TASK_PROCESSING;TASK_DONE
	InputFileName         string
	IntermediateFileNames []string
	Taskname              int
	NReduce               int // same as Master's

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HandOutTask(args *HandOutTaskArgs, reply *HandOutTaskReply) error {
	// if the map task hasn't done,do map task first
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state < 1 {
		for _, task := range c.mapTask {
			if task.State == 0 {
				task.State = 1
				reply.Y = *task
				go c.Check(1, task.Taskname)
				return nil
			}
		}
	} else {
		for _, v := range c.reduceTask {
			if v.State == 0 {
				v.State = 1
				reply.Y = *v
				go c.Check(2, v.Taskname)
				return nil
			}
		}
	}
	reply.Y = Task{}
	return fmt.Errorf("tasks have hand out all")
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == 1 {
		// late's task
		if c.mapTask[args.TaskName].State == 2 {
			return nil
		}
		taskName := args.TaskName
		c.mapTask[taskName].State = 2
		for _, filename := range args.FileNames {
			index := strings.LastIndex(filename, "-")
			num, _ := strconv.Atoi(filename[index+1:])
			if v, ok := c.reduceTask[num]; ok {
				v.IntermediateFileNames = append(v.IntermediateFileNames, filename)
			} else {
				task := &Task{
					TaskType:              2,
					IntermediateFileNames: []string{filename},
					Taskname:              num,
				}
				c.reduceTask[num] = task
			}
		}
		c.mapTaskFinishNum++
		if c.mapTaskFinishNum == len(c.mapTask) {
			c.state = 1
		}
	} else {
		// late's task
		if c.reduceTask[args.TaskName].State == 2 {
			return nil
		}
		taskName := args.TaskName
		c.reduceTask[taskName].State = 2
		c.reduceTaskFinishNum++
		if c.reduceTaskFinishNum == len(c.reduceTask) {
			c.state = 2
		}
	}
	return nil
}

func (c *Coordinator) Check(taskType int, taskName int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if taskType == 1 {
		if c.mapTask[taskName].State == 1 {
			c.mapTask[taskName].State = 0
		}
	} else {
		if c.reduceTask[taskName].State == 1 {
			c.reduceTask[taskName].State = 0
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
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
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == 2 {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state:      0,
		nReduce:    nReduce,
		nMap:       40, //假设开4个worker
		mapTask:    []*Task{},
		reduceTask: map[int]*Task{},
	}

	// Your code here.
	for i, filename := range files {
		newTask := &Task{
			Taskname:      i,
			TaskType:      1,
			State:         0,
			InputFileName: filename,
			NReduce:       nReduce,
		}
		c.mapTask = append(c.mapTask, newTask)
	}

	c.server()
	return &c
}
