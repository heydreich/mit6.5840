package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	alive := true
	for alive {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(&task, mapf)
				TaskDone(&task)
			}
		case WaitingTask:
			{
				fmt.Println("All tasks are in progress , Worker wait")
				time.Sleep(time.Second)
			}
		case ReduceTask:
			{
				DoReduceTask(&task, reducef)
				TaskDone(&task)
			}
		case ExitTask:
			{
				fmt.Println("All tasks are down , Worker exit")
				alive = false
			}
		}
	}
}

func TaskDone(task *Task) {
	args := task
	reply := Task{}
	fmt.Println("MarkDone()")
	ok := call("Coordinator.MarkDone", args, &reply)
	if ok {
		fmt.Println("Task Done!")
	}
}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	// fmt.Println("gettask()")
	if ok := call("Coordinator.PullTask", &args, &reply); ok {
		fmt.Printf("reply TaskId is %d\n", reply.TaskId)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	fmt.Println(task.FileName)
	file, err := os.Open(task.FileName[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := io.ReadAll((file))
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	reduceNum := task.ReduceNum
	intermediate = mapf(task.FileName[0], string(content))
	HashKv := make([][]KeyValue, reduceNum)
	for _, v := range intermediate {
		index := ihash(v.Key) % reduceNum
		HashKv[index] = append(HashKv[index], v)
	}
	path, _ := os.Getwd()
	for i := 0; i < reduceNum; i++ {
		os.Mkdir("./mr-tmp-"+strconv.Itoa(i), 0666)
		os.Chmod("./mr-tmp-"+strconv.Itoa(i), 0777)
	}

	for i := 0; i < reduceNum; i++ {

		filename := path + "/mr-tmp-" + strconv.Itoa(i) + "/mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		new_file, err := os.Create(filename)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		enc := json.NewEncoder(new_file)
		for _, kv := range HashKv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		new_file.Close()
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	reduceNum := task.TaskId

	intermediate := shuffle(task.FileName)

	finalName := fmt.Sprintf("mr-out-%d", reduceNum)
	ofile, err := os.Create(finalName)
	if err != nil {
		log.Fatal("create file failed:", err)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
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

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

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
