package mr

import  (
	"fmt"
 	"log"
 	"net/rpc"
 	"hash/fnv"
 	"os"
	 "io/ioutil"
	 "bufio"
	 "strings"
	 "sort"
	 "time"
)




//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

type File struct {
    // contains filtered or unexported fields
}

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
<<<<<<< HEAD
	// uncomment to send the Example RPC to the master.
	//CallExample()

	//Step 1: request for a job using RPC (the job can be )
	var reply ReplyJob
	reply  = RequestJob()

	quit_counter := 0


	//One while loop which will only terminate when T == 2, i.e., when the whole job is done
	for reply.Terminate == false || quit_counter < 10 {

		//Map Job
		if reply.T == 0 {
			//fmt.Println("I got a map job for file!", reply.F)

			//
			// read the input file,
			// pass it to Map,
			// accumulate the intermediate Map output.
			//

			intermediate := []KeyValue{}
			file, err := os.Open(reply.F)
			if err != nil {
				log.Fatalf("cannot open %v", reply.F)
			 }
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.F)
			}
			file.Close()
			kva := mapf(reply.F, string(content))
			intermediate = append(intermediate, kva...)
			//sort.Sort(KeyValue(intermediate))

			//Now need to use the hash function for each key/word in intermediate and put them into buckets
			//First creat an array of pointers to the intermediate files
			inter_files := make([] (*os.File), reply.R)
			for i := 0; i < reply.R; i++ {
				filename := fmt.Sprintf("mr_%v_%v", reply.W, i)
				file, err := os.Create(filename)
				if err != nil{
					log.Fatalf("cannot create %v", filename)
				}
				inter_files[i] = file
			}

			//Then look through <key, value> pairs in intermediate and put them into the buckets (intermediate files)
			for _, s := range intermediate {
				bucket := ihash(s.Key) % reply.R
				fmt.Fprintf(inter_files[bucket], "%v %v \n", s.Key, s.Value)

				if s.Key == "Additional" {
					fmt.Println("Mapper: found ", s.Key)
					fmt.Println("Storing it at: ", &(inter_files[bucket]))
				}
			}

			//Let the master know that we are done through an RPC
			dummy :=MapDone(reply.W, reply.F)
			dummy.W = 0

		
		}
		
		//Reduce Job
		if reply.T == 1{

			//fmt.Println("I got a reduce job, bucket: ", reply.Bucket)
			//Open the intermediate files associated with the assigned bucket # (reply.Bucket)
			// and store them in kva
			kva := [] KeyValue{}
			//inter_files := make([] (*os.File), reply.R)
			for j:=0; j < reply.R ; j++ {
				filename := fmt.Sprintf("mr_%v_%v", j, reply.Bucket)
				file, err := os.Open(filename)
				scanner := bufio.NewScanner(file)
				if err != nil{
					log.Fatalf("cannot open %v", filename)
				}

				scanner.Split(bufio.ScanLines)
				var text string 

				for scanner.Scan() {
					text = scanner.Text()
					//fmt.Println("scanned text: ", text)
					s := strings.Split(text, " ")
					a := KeyValue{}
					a.Key = s[0]
					a.Value = s[1]
					kva = append(kva, a)

					if a.Key == "Additional" {
						fmt.Println("Reducer: found ", a.Key)
						fmt.Println("Found it from: ", file)
					}

				}
				//inter_files[j] = file
				file.Close()

			}
			//fmt.Println("here is the kva after opening intermediate files:")
			//fmt.Println(kva)
			

			//Then process each intermediate file and append the reduce output
			sort.Sort(ByKey(kva))
			out := fmt.Sprintf("mr-out-%v", reply.Bucket)
			ofile, _ := os.Create(out)
=======

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
>>>>>>> official/master

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()

			//Send an RPC to Master to notify of the reduce task done
			dummy := ReduceDone(reply.Bucket)
			dummy.W = 0
		}

		//2 means that no work is available, hence the worker should wait and then request a job again
		if reply.T == 2 {
			//fmt.Println("Worker: no task given, need to sleep!")
			time.Sleep(1*time.Second)
			quit_counter ++
		}

		//fmt.Println("requesting a new job: ", reply.W)
		reply  = RequestJob()
	}
	

	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func RequestJob() ReplyJob {

	args := Request{}
	reply := ReplyJob{}

	call("Master.AllocateJob", &args, &reply)
	//fmt.Println("reply.F ", reply.F)
	//fmt.Println("type of job: ", reply.T)

	return reply
}

func MapDone(worker_id int, file string) ReplyJob {

	args := MapTaskDone{}
	args.Worker = worker_id
	args.File = file
	reply := ReplyJob{}

	call("Master.MapTaskDone", &args, &reply)

	return reply
}

func ReduceDone(bucket_num int) ReplyJob{

	args := ReduceTaskDone{}
	args.Bucket = bucket_num

	reply := ReplyJob{}

	call("Master.ReduceTaskDone", &args, &reply)

	return reply
}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
