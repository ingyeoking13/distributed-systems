package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type JobType int

const (
	MapType    JobType = 0
	ReduceType JobType = 1
	FIN        JobType = 2
)

type JobArgs struct {
	Idx      int
	JobType  JobType
	FileName string
}

type JobReply struct {
	JobType           JobType
	Idx               int
	FileName          string
	IntermediateFiles []string
	NReduce           int
}

type FinJobArgs struct {
	Idx          int
	JobType      JobType
	OriFileNames []string
	ResFileNames []string
}

type FinJobReply struct {
	Good bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
