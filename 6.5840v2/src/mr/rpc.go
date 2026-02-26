package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Add your RPC definitions here.

type JobType int

const (
	MapType    JobType = 0
	ReduceType JobType = 1
	WAIT       JobType = 2
	FIN        JobType = 3
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
