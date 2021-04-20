package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	READY TaskState = iota
	RUNNING
	COMPLETE
)

const (
	MAP TaskType = iota
	REDUCE
	EMPTY
	EXIT
)

type TaskState int
type TaskType int

func (ts TaskState) String() string {
	switch ts {
	case READY:
		return "READY"
	case RUNNING:
		return "RUNNING"
	case COMPLETE:
		return "COMPLETE"
	default:
		return "Unknown TaskState"
	}
}

func (tt TaskType) String() string {
	switch tt {
	case MAP:
		return "MapTask"
	case REDUCE:
		return "ReduceTask"
	default:
		return "Unknown TaskType"
	}
}

type Task struct {
	Id       int
	TaskType TaskType
	State    TaskState
	Key      string // filename of map input
}
type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// get available map task
	mapTasksDone := 0
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.State == READY {
			task.State = RUNNING
			reply.TaskId = task.Id
			reply.TaskType = MAP
			reply.Key = task.Key
			reply.NReduce = len(c.reduceTasks)
			go c.waitForTask(task)
			return nil
		} else if task.State == COMPLETE {
			mapTasksDone++
		}
	}

	// only start reduce tasks once all map tasks are done
	reduceTasksDone := 0
	if mapTasksDone == len(c.mapTasks) {
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.State == READY {
				task.State = RUNNING
				reply.TaskId = task.Id
				reply.TaskType = REDUCE
				go c.waitForTask(task)
				return nil
			} else if task.State == COMPLETE {
				reduceTasksDone++
			}
		}
	}

	// wait til all tasks are complete
	if mapTasksDone != len(c.mapTasks) || reduceTasksDone != len(c.reduceTasks) {
		reply.TaskType = EMPTY
		return nil
	}

	reply.TaskType = EXIT

	return nil
}

func (c *Coordinator) SetTaskComplete(args *SetTaskCompleteArgs, reply *SetTaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if args.TaskType == MAP {
		task = &c.mapTasks[args.TaskId]
	} else if args.TaskType == REDUCE {
		task = &c.reduceTasks[args.TaskId]
	}

	if task.State == RUNNING {
		task.State = COMPLETE
	} else {
		return errors.New("trying to complete a task that is not running")
	}

	return nil
}

func (c *Coordinator) waitForTask(task *Task) {
	<-time.After(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	if task.State == RUNNING {
		log.Printf("%v-%v failed/timed-out, resetting\n", task.TaskType, task.Id)
		task.State = READY
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	nComplete := 0

	// check map tasks
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.State == COMPLETE {
			nComplete++
		}
	}

	// check reduce tasks
	for i := range c.reduceTasks {
		task := &c.reduceTasks[i]
		if task.State == COMPLETE {
			nComplete++
		}
	}

	return nComplete == len(c.mapTasks)+len(c.reduceTasks)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// create map tasks
	mapTasks := []Task{}
	nMap := len(files)
	for i := 0; i < nMap; i++ {
		mapTasks = append(mapTasks, Task{Id: i, TaskType: MAP, State: READY, Key: files[i]})
	}

	reduceTasks := []Task{}
	for i := 0; i < nReduce; i++ {
		reduceTasks = append(reduceTasks, Task{Id: i, TaskType: REDUCE, State: READY})
	}

	c := Coordinator{mapTasks: mapTasks, reduceTasks: reduceTasks}

	c.server()
	return &c
}
