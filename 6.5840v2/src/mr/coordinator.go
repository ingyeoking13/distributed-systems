package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type MapJob struct {
	id        int
	splitFile string
	done      bool
	startTime time.Time
}

type ReducerJob struct {
	partitionId int
	done        bool
	startTime   time.Time
}

type Coordinator struct {
	// Your definitions here.

	muLock               sync.Mutex
	nReduce              int
	mapJobs              []MapJob
	reducerJobs          map[int]*ReducerJob
	intermediateFiles    [][]string
	remainedReducerCount int
	remainedMapCount     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetJob(args *JobArgs, reply *JobReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	// 만약 모든 job이 map이 끝나지 않았다면
	if c.remainedMapCount > 0 {
		for i := 0; i < len(c.mapJobs); i++ {
			if c.mapJobs[i].done == true {
				continue
			}

			if c.mapJobs[i].startTime.Add(10 * time.Second).After(time.Now()) {
				continue
			}

			job := &c.mapJobs[i]
			job.startTime = time.Now()

			reply.JobType = MapType
			reply.NReduce = c.nReduce
			reply.Idx = job.id
			reply.FileName = job.splitFile
			return nil
		}
		reply.JobType = WAIT
		return nil
	}

	if c.remainedReducerCount > 0 {
		for i := 0; i < c.nReduce; i++ {
			if c.reducerJobs[i].done == true {
				continue
			}

			if c.reducerJobs[i].startTime.Add(10 * time.Second).After(time.Now()) {
				continue
			}

			job := c.reducerJobs[i]
			job.startTime = time.Now()

			reply.JobType = ReduceType
			reply.Idx = job.partitionId
			reply.IntermediateFiles = c.intermediateFiles[job.partitionId]
			return nil
		}

		reply.JobType = WAIT
		return nil
	}

	reply.JobType = FIN
	return nil
}

func (c *Coordinator) FinJob(args *FinJobArgs, reply *FinJobReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	reply.Good = true
	if args.JobType == MapType {
		if c.mapJobs[args.Idx].done == true {
			return nil
		}

		c.mapJobs[args.Idx].done = true
		c.remainedMapCount--

		if c.intermediateFiles == nil {
			c.intermediateFiles = make([][]string, c.nReduce)
		}

		re := regexp.MustCompile(`(\d+)$`)
		for _, resFileName := range args.ResFileNames {
			id, _ := strconv.Atoi(re.FindStringSubmatch(resFileName)[0])
			c.intermediateFiles[id] = append(c.intermediateFiles[id], resFileName)

		}
		return nil
	}

	// else reduce job finished

	if c.reducerJobs[args.Idx].done == true {
		return nil
	}
	c.remainedReducerCount--
	c.reducerJobs[args.Idx].done = true
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
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.muLock.Lock()
	defer c.muLock.Unlock()

	return c.remainedReducerCount == 0 && c.remainedMapCount == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	co := Coordinator{}
	co.nReduce = nReduce
	co.remainedMapCount = len(files)
	co.remainedReducerCount = nReduce

	for idx, file := range files {
		var newJob MapJob
		newJob.id = idx
		newJob.splitFile = file
		newJob.done = false
		co.mapJobs = append(co.mapJobs, newJob)

	}

	co.reducerJobs = make(map[int]*ReducerJob)

	for i := 0; i < nReduce; i++ {
		co.reducerJobs[i] = &ReducerJob{
			partitionId: i,
			done:        false,
		}
	}

	co.server(sockname)
	return &co
}
