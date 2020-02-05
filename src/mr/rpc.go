package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.
type FetchTaskArgs struct {
}

type FetchTaskReply struct {
	TaskID   int
	Input    []string
	Type     TaskType
	NReduce  int
	Finished bool
}

type SubmitTaskArgs struct {
	TaskID int
	Type   TaskType
	Output []string
}

type SubmitTaskReply struct {
}
