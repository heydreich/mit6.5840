package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"

	"hash/fnv"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	rargs := RegisterArgs{}
	rreply := RegisterReply{}

	ok := call("Coordinator.RegisterWorker", &rargs, &rreply)
	if !ok {
		log.Fatal("InWorker error!")
	}
	workerId := rreply.Workerid

	// Your worker implementation here.
	for {
		args := GetTaskArgs{Workerid: workerId}
		reply := GetTaskReply{}

		//this will wait until we get assigned a task
		ok := call("Coordinator.HandleGetTask", &args, &reply)
		if !ok {
			log.Fatal("HandleGetTask error!")
		}
		fmt.Println("reply.TaskType", reply.TaskType)

		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %s", reply.TaskType)
		}

		finArgs := &FinishedTaskArgs{
			Workid:   workerId,
			Tasktype: reply.TaskType,
			TaskNum:  reply.TaskNum,
		}
		finReply := &FinishedTaskReply{}
		ok = call("Coordinator.HandleFinishedTask", finArgs, finReply)
		if !ok {
			log.Fatal("HandleFinishedTask error!")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func performMap(mapFile string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue) {
	var intermediate = []KeyValue{}
	fmt.Println(mapFile)
	file, err := os.Open(mapFile)
	if err != nil {
		log.Fatalf("cannot open %v", mapFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapFile)
	}
	file.Close()
	kva := mapf(mapFile, string(content))
	intermediate = append(intermediate, kva...)
	Hashkv := make([][]KeyValue, nReduceTasks)
	for _, v := range intermediate {
		index := ihash(v.Key) % nReduceTasks
		Hashkv[index] = append(Hashkv[index], v)
	}
	path, _ := os.Getwd()
	for i := 0; i < nReduceTasks; i++ {
		os.Mkdir("./mr-tmp-"+strconv.Itoa(i), 0666)
		os.Chmod("./mr-tmp-"+strconv.Itoa(i), 0777)
	}

	for i := 0; i < nReduceTasks; i++ {
		filename := path + "/mr-tmp-" + strconv.Itoa(i) + "/mr-tmp-" + strconv.Itoa(taskNum) + "-" + strconv.Itoa(i)
		new_file, err := os.Create(filename)
		if err != nil {
			log.Fatal("create file failed:", err)
		}

		enc := json.NewEncoder(new_file)
		for _, kv := range Hashkv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		new_file.Close()
	}
}

func performReduce(taskNum int, nMapTasks int, reducef func(string, []string) string) {

	intermediate := shuffle(GetReduceTasks(taskNum))

	finalName := fmt.Sprintf("mr-out-%d", taskNum)
	ofile, _ := os.Create(finalName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

func shuffle(files []string) []KeyValue {
	// for _, ss := range files {
	// 	fmt.Println(ss)
	// }
	kva := []KeyValue{}
	for _, fi := range files {
		// fmt.Println(fi)
		file, err := os.Open(fi)
		if err != nil {
			log.Fatalf("cannot open %v", fi)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func GetReduceTasks(id int) []string {
	s := []string{}
	path, _ := os.Getwd()

	files, _ := os.ReadDir(path + "/mr-tmp-" + strconv.Itoa(id))
	for _, f := range files {
		finame := f.Name()
		finame = "mr-tmp-" + strconv.Itoa(id) + "/" + finame
		// fmt.Println(finame)
		s = append(s, finame)
	}
	return s
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

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
