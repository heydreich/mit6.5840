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

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

type GetTaskArgs struct {
	Workerid int
}

type GetTaskReply struct {
	//what type of this task
	TaskType TaskType
	//task number of either map or reduce task
	TaskNum int
	//needed for Map (which file to write)
	NReduceTasks int
	//needed for Map (which file to read)
	MapFile string
	//needed for reduce (to know how many intermediate map files to read)
	NMapTasks int
}

type FinishedTaskArgs struct {
	Workid int
	//type of task worker assigned
	Tasktype TaskType
	// which task is it ? taskid
	TaskNum int
}

type FinishedTaskReply struct{}

type RegisterArgs struct{}

type RegisterReply struct {
	Workerid int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
