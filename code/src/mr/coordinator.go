package mr

import ( "log"
 "net"
 "os"
 "net/rpc"
 "net/http"
 "fmt"
 "sync"
"time"

)

<<<<<<< HEAD:src/mr/master.go
type Master struct {
=======

type Coordinator struct {
	// Your definitions here.
>>>>>>> official/master:src/mr/coordinator.go

	// Your definitions here.
	map_Assigned []bool				//an array of booleans to indicate which map file has been ASSIGNED
	map_Done []bool					//an array of booleand to indicate which map files is DONE
	map_files []string				//an array containing the filenames
	done_work bool								//a flag to indicate whether the WHOLE work is done
	num_map_files int							//the number of files to map
	num_reduce int								//number of reduce files
	map_counter int
	map_done bool
	reduce_done bool
	reduce_counter int
	key sync.Mutex					//a mutex to make sure simultaneous RPC calls are handled properly
	
	reduce_Assigned []bool			//an array of glags keeping track of the reduce tasks ASSIGNED
	reduce_Done []bool				//an array of flags keeping track of the reduce tasks DONE
	}	

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

<<<<<<< HEAD:src/mr/master.go
func (m *Master) AllocateJob(args *Request, replyJob *ReplyJob) error {

	//use the key to lock access to the data, to avoid multiple RPC handler races
	m.key.Lock()
	defer m.key.Unlock()
	if m.done_work == false {

		replyJob.Terminate = false

		if m.map_done == false {
			
			var k int
			for k = 0; k < m.num_map_files; k++ {

				//Map work is found
				if m.map_Assigned[k] == false {
					replyJob.F = m.map_files[k]						//send the worker the corresponding file
					replyJob.T = 0									//identify to the worker that it is a MAP task
					replyJob.R = m.num_reduce						//send the # of reduce files to make (used for intermediate files)
					replyJob.W = k									//tell the user the # of mappers corresponding to each file (used for intermediate files)
					m.map_Assigned[k] = true						//mark the assigned array for the file as TRUE	
					
					//Start a go routine (a thread that will wait 10 seconds for to get a reply from the mapper or else re-assign its work to a different mapper)
					go func(id int) {

						time.Sleep(10* time.Second)
						m.key.Lock()
						if m.map_Done[id] == false {
							m.map_Assigned[id] = false
						}
						m.key.Unlock()
					} (replyJob.W)

					break
				}
				
				//All Map work has been given out
				if k == (m.num_map_files - 1) {
					if m.map_Assigned[k] == true {
						replyJob.T = 2
					}
				}
			}					
			
			//Lastly iterate through the map_Done array in order to check whether all map tasks have been completed
			m.map_counter = 0
			for z:= 0 ; z < len(m.map_Done); z++ {
				if m.map_Done[z] == false {
					break
				}else {
					m.map_counter ++
				}
			}

			//If they have been completed change the boolean of map_done to true
			if m.map_counter == len(m.map_Done) {
				fmt.Println("All map tasks have been completed!")
				m.map_done = true
			}

			//Look if there are reduce tasks to be given out
		}	else if  m.reduce_done == false  {
			
			//fmt.Println("Beginning Reduce Task")
			var k int
			for k = 0; k < m.num_reduce; k++ {

				//Reduce Work is found
				if m.reduce_Assigned[k] == false {
					m.reduce_Assigned[k] = true
					replyJob.T = 1
					replyJob.Bucket = k
					replyJob.R = m.num_map_files
					//Start a go routine (a thread that will wait 10 seconds for to get a reply from the mapper or else re-assign its work to a different mapper)
					go func(id int) {


						time.Sleep(10* time.Second)
						m.key.Lock()
						if m.reduce_Done[id] == false {
							m.reduce_Assigned[id] = false
						}
						m.key.Unlock()
					} (replyJob.Bucket)

					break
				}

				//All reduce tasks have been given out
				if k == (m.num_reduce -1) {
					if m.reduce_Assigned[k] == true {
						replyJob.T = 2
					}
				}
			}

			//Check is all Reduce tasks have been done
			m.reduce_counter = 0
			for z:= 0 ; z < len(m.reduce_Done); z++ {
				if m.reduce_Done[z] == false {
					break
				}else {
					m.reduce_counter ++
				}
			}

			//If they have been completed change the boolean of map_done to true
			if m.reduce_counter == len(m.reduce_Done) {
				fmt.Println("All reduce tasks have been completed!")
				m.reduce_done = true
			}

			//Check if the whole task is done
			if m.reduce_done == true && m.map_done == true {
				fmt.Println("The whole task has been completed!")
				m.done_work = true
			}
			//Last option: All tasks have been given out and the workr should wait
		} else {

			//fmt.Println("No tasks to be given out")
			//fmt.Println("Map tasks are: ", m.map_done)
			//fmt.Println("Reduce tasks are: ", m.reduce_done)

			replyJob.T = 2
		}
	} else {
		replyJob.Terminate = true
	}
	

	return nil
}

func (m *Master) MapTaskDone(args *MapTaskDone, reply *ReplyJob) error {
	
	m.key.Lock()
	
	fmt.Println("I got an RPC from a mapper: ", args.Worker, " who is done with: ", args.File)

	m.map_Done[args.Worker] = true
	m.map_Assigned[args.Worker] = true

	m.key.Unlock()

	return nil
}

func (m *Master) ReduceTaskDone(args *ReduceTaskDone, reply *ReplyJob) error {
	m.key.Lock()
	
	fmt.Println("I got an RPC from a reducer who is done with bucket: ", args.Bucket)

	m.reduce_Done[args.Bucket] = true
	m.reduce_Assigned[args.Bucket] = true

	m.key.Unlock()
	
	return nil
}
=======

>>>>>>> official/master:src/mr/coordinator.go
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
<<<<<<< HEAD:src/mr/master.go
func (m *Master) Done() bool {
	
	m.key.Lock()
	ret := m.done_work

	if ret == true {
		fmt.Println("The task is done!")
	}
	
	
	m.key.Unlock()
=======
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


>>>>>>> official/master:src/mr/coordinator.go
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	m.num_map_files = len(files)						
	m.done_work = false
	m.map_done = false
	m.reduce_done = false
	m.num_reduce = nReduce
	m.map_counter = 0
	m.map_files = files
	m.reduce_counter = 0
	m.key = sync.Mutex{}

	//Initialize the boolean arrays for both mappers and reducers
	m.map_Assigned = make([]bool, m.num_map_files)
	m.map_Done = make([]bool, m.num_map_files)

	m.reduce_Assigned = make([]bool, m.num_reduce)
	m.reduce_Done = make([]bool, m.num_reduce)

<<<<<<< HEAD:src/mr/master.go
	fmt.Println("num of map files: ", m.num_map_files)
	fmt.Println("The array of flags: ", m.map_Assigned)
	fmt.Println("Number of reduce files: ", m.num_reduce)
	fmt.Println("All files my copy: ", m.map_files)
	fmt.Println("Files received from term. : ", files)
	
	m.server()
	return &m
=======

	c.server()
	return &c
>>>>>>> official/master:src/mr/coordinator.go
}
