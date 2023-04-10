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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	MapTask    TaskType = 1
	ReduceTask TaskType = 2
	DoneTask   TaskType = 3
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
	TaskType TaskType
	TaskId   int
}

type TaskReply struct {
	TaskType TaskType

	TaskId   int
	NMapper  int
	NReducer int

	MapFile string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
