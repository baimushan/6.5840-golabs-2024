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

// example to show how to declare the arguments
// and reply for an RPC.
const (
	WorkerTypeMap = iota
	WorkerTypeReduce
)

type WorkerType int

type JobArgs struct {
	X int
}

type JobReply struct {
	Y int

	Type WorkerType

	FileName string // map job file name ; outfile name for reduce
	Nreduce  int    // for map job : number of reduce tasks
	Nmap     int    // for reduce job : number of map tasks
	TaskIdx  int

	ReduceFiles []string // for reduce worker
}

type JobStatusArgs struct {
	Type     WorkerType
	FileName string
}

type JobStatusReply struct {
	Y int
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
