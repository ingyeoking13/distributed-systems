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
	id int
	file string
	state TaskState
	taskType TaskType
	startTime time.Time
}


type Coordinator struct {
	// Your definitions here.
	nReduce int
	mutex 	sync.Mutex
	mapTasks []Task
	reduceTasks  []Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandler(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Done {
		if args.TaskType == MapTask {
			c.mapTasks[args.Id].state = Completed
			return nil
		}
		if args.TaskType == ReduceTask {
			c.reduceTasks[args.Id].state = Completed
			return nil
		}
	}

	reply.NReduce = c.nReduce

	finished := 0
	for i:=0; i< len(c.mapTasks); i++ {
		state := c.mapTasks[i].state
		if (state == Idle) {
			reply.Id = c.mapTasks[i].id
			reply.TaskType = c.mapTasks[i].taskType
			reply.File = c.mapTasks[i].file
			c.mapTasks[i].state = InProgress
			c.mapTasks[i].startTime = time.Now()
			return nil
		} else if (state == Completed) {
			finished++
		}
	}

	if (finished < len(c.mapTasks)) {
		reply.Id = -1
		reply.TaskType = Wait
		return nil
	}

	finished = 0
	for i:=0; i< len(c.reduceTasks); i++ {
		state := c.reduceTasks[i].state
		if (state == Idle) {
			reply.Id = c.reduceTasks[i].id
			reply.TaskType = c.reduceTasks[i].taskType
			c.reduceTasks[i].state = InProgress
			c.reduceTasks[i].startTime = time.Now()
			return nil
		} else if (state == Completed) {
			finished++
		} 
	}

	if (finished < len(c.reduceTasks)) {
		reply.TaskType = Wait
		return nil
	}
	reply.TaskType = Exit
	return nil

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := false
	// Your code here.
	for _, task := range c.reduceTasks {
		if task.state != Completed {
			return ret
		}
	}
	ret = true
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		nReduce: nReduce,
	}

	for i := 0; i < len(files); i++ {
		c.mapTasks = append(c.mapTasks, Task{ 
			file: files[i],
			id: i,
			state: Idle,
			taskType: MapTask,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			id: i,
			state: Idle,
			taskType: ReduceTask,
		})
	}

	c.server()
	return &c
}
