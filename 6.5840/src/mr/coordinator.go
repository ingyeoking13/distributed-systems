package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int
type TaskType int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
	Wait
	Exit
)

type Task struct {
	Id        int
	File      string
	State     TaskState
	TaskType  TaskType
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	NReduce     int
	Mutex       sync.Mutex
	MapTasks    []Task
	ReduceTasks []Task
	MapDone     int
	ReduceDone  int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) JobRequest(args *TaskArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	reply.NReduce = c.NReduce

	if c.MapDone < len(c.MapTasks) {
		for i := 0; i < len(c.MapTasks); i++ {
			state := c.MapTasks[i].State
			startTime := c.MapTasks[i].StartTime
			switch state {
			case Idle:
				reply.Id = c.MapTasks[i].Id
				reply.TaskType = c.MapTasks[i].TaskType
				reply.File = c.MapTasks[i].File
				c.MapTasks[i].State = InProgress
				c.MapTasks[i].StartTime = time.Now()
				return nil
			case InProgress:
				tenSecondsAgo := time.Now().Add(-10 * time.Second)
				if startTime.Before(tenSecondsAgo) {
					reply.Id = c.MapTasks[i].Id
					reply.TaskType = c.MapTasks[i].TaskType
					reply.File = c.MapTasks[i].File
					c.MapTasks[i].State = InProgress
					c.MapTasks[i].StartTime = time.Now()
					return nil
				}
			}
		}
		reply.Id = -1
		reply.TaskType = Wait
		return nil
	}

	if c.ReduceDone < len(c.ReduceTasks) {
		for i := 0; i < len(c.ReduceTasks); i++ {
			state := c.ReduceTasks[i].State
			startTime := c.ReduceTasks[i].StartTime
			switch state {
			case Idle:
				reply.Id = c.ReduceTasks[i].Id
				reply.TaskType = c.ReduceTasks[i].TaskType
				c.ReduceTasks[i].State = InProgress
				c.ReduceTasks[i].StartTime = time.Now()
				return nil
			case InProgress:
				tenSecondsAgo := time.Now().Add(-10 * time.Second)
				if startTime.Before(tenSecondsAgo) {
					reply.Id = c.ReduceTasks[i].Id
					reply.TaskType = c.ReduceTasks[i].TaskType
					reply.File = c.ReduceTasks[i].File
					c.ReduceTasks[i].State = InProgress
					c.ReduceTasks[i].StartTime = time.Now()
					return nil
				}
			}
		}
		reply.TaskType = Wait
		return nil
	}

	reply.TaskType = Exit
	return nil
}

func (c *Coordinator) RPCDone(args *TaskArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if !args.Done {
		return nil
	}

	if args.TaskType == MapTask {
		c.MapTasks[args.Id].State = Completed
		c.MapDone++
		return nil
	}
	if args.TaskType == ReduceTask {
		c.ReduceTasks[args.Id].State = Completed
		c.ReduceDone++
		return nil
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// Your code here.
	for _, task := range c.ReduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		NReduce: nReduce,
	}

	for i := 0; i < len(files); i++ {
		c.MapTasks = append(c.MapTasks, Task{
			File:     files[i],
			Id:       i,
			State:    Idle,
			TaskType: MapTask,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, Task{
			Id:       i,
			State:    Idle,
			TaskType: ReduceTask,
		})
	}

	c.server()
	return &c
}
