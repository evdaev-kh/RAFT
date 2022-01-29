package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type Request struct {
	I int //id of the worker
}

type ReplyJob struct {
	T int		//type of job: 0 for map, 1 for reduce, 3 for the overall job being done.
	F string	//filename: for either map or reduce task
	R int		//the # of reduce files
	W int		//the # map worker
	Bucket int	//the assigned bucket number for a reduce job/worker
	Terminate bool //to tell the worker to shut off when the whole map&reduce task is done
}

type MapTaskDone struct {
	Worker int	//the worker # who worked on the corresponding file #
	File string
}

type ReduceTaskDone struct {
	Bucket int 		//the bucket #
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
