package mr

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"net/http"
	"net/rpc"
)

// type Coordinator struct {
// 	// Your definitions here.

// 	//protect coordinator state
// 	//from concurrent access
// 	mu sync.Mutex

// 	//Allow coordinator to wait to assign reduce tasks until map tasks have finished
// 	//ro when all tasks are assigned and are running
// 	//The coordinator is woken up either when a task has finished , or if a timeout has expired
// 	cond *sync.Cond

// 	//len(mapFiles) == nMap
// 	mapFiles     []string
// 	nMapTasks    int
// 	nReduceTasks int

// 	//keep track of when tasks are assigned
// 	//which tasks have finished
// 	mapTasksFinished    []bool
// 	mapTasksIssued      []time.Time
// 	reduceTasksFinished []bool
// 	reduceTasksIssued   []time.Time

// 	//set to true if all reduce tasks are complete
// 	isDone bool
// }

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// handle GetTask RPCs from worker
// func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()

// 	reply.NReduceTasks = c.nReduceTasks
// 	reply.NMapTasks = c.nMapTasks

// 	for {
// 		var mapDone = true
// 		for m, done := range c.mapTasksFinished {
// 			if !done {
// 				//Assign a task if it's either never been issued, or if it's been too long
// 				//since it was issued so the worker may have crashed
// 				//Note: if task has never been issued, time is initialized to 0 UTC
// 				if c.mapTasksIssued[m].IsZero() || time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
// 					reply.TaskType = Map
// 					reply.TaskNum = m
// 					reply.MapFile = c.mapFiles[m]
// 					c.mapTasksIssued[m] = time.Now()
// 					return nil
// 				} else {
// 					mapDone = false
// 				}
// 			}
// 		}

// 		if !mapDone {
// 			//TODO wait
// 			c.cond.Wait()
// 		} else {
// 			//all map tasks done
// 			break
// 		}
// 	}

// 	//reduce task state
// 	for {
// 		var reduceDone = true
// 		for r, done := range c.reduceTasksFinished {
// 			if !done {
// 				if c.reduceTasksIssued[r].IsZero() || time.Since(c.reduceTasksIssued[r]).Seconds() > 10 {
// 					reply.TaskType = Reduce
// 					reply.TaskNum = r
// 					c.reduceTasksIssued[r] = time.Now()
// 					return nil
// 				} else {
// 					reduceDone = false
// 				}
// 			}

// 		}
// 		// fmt.Println(c.reduceTasksFinished)
// 		if !reduceDone {
// 			c.cond.Wait()
// 		} else {
// 			break
// 		}

// 	}

// 	reply.TaskType = Done
// 	c.isDone = true
// 	return nil
// }

// func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()
// 	switch args.Tasktype {
// 	case Map:
// 		c.mapTasksFinished[args.TaskNum] = true
// 	case Reduce:
// 		c.reduceTasksFinished[args.TaskNum] = true
// 	default:
// 		log.Fatal("Bad finished task? %s", args.Tasktype)
// 	}
// 	c.cond.Broadcast()

// 	return nil
// }

var mu sync.Mutex
var mapFiles []string
var nMapTasks int
var nReduceTasks int
var waitWorkers = make(chan int)

var workerReplys []chan *GetTaskReply
var workerFinisheds []chan *FinishedTaskArgs

var numWokers int
var repTask chan *GetTaskReply

// set to true if all reduce tasks are complete
var isDone bool

func DoCoordinator(workers chan int, numTask int, call_worker func(worker int, task int) bool) {
	tasks := make(chan int, numTask)
	done := make(chan bool)
	exit := make(chan struct{})

	go func() {
		for {
			select {
			case Worker := <-workers:
				go issueWorkerTaskThread(Worker, done, tasks, call_worker)
			case <-exit:
				return
			}
		}
	}()

	for task := 0; task < numTask; task++ {
		tasks <- task
	}
	for i := 0; i < numTask; i++ {
		<-done
	}

	close(tasks)

	exit <- struct{}{}
}

func issueWorkerTaskThread(
	worker int,
	done chan bool,
	tasks chan int,
	call_worker func(worker int, task int) bool) {
	for task := range tasks {
		if call_worker(worker, task) {
			done <- true
		} else {
			tasks <- task
		}
	}
}

func IssueMapTask(worker int, task int) bool {
	// fmt.Println("worker:", worker)
	// fmt.Println(len(workerReplys))
	reply := GetTaskReply{
		TaskType:     Map,
		TaskNum:      task,
		MapFile:      mapFiles[task],
		NReduceTasks: nReduceTasks,
		NMapTasks:    nMapTasks,
	}
	workerReplys[worker] <- &reply

	<-workerFinisheds[worker]
	return true
}

func IssueRedTask(worker int, task int) bool {
	reply := GetTaskReply{
		TaskType:     Reduce,
		TaskNum:      task,
		NReduceTasks: nReduceTasks,
		NMapTasks:    nMapTasks,
	}
	workerReplys[worker] <- &reply
	// start := time.Now()

	<-workerFinisheds[worker]
	return true
}

func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	workerid := args.Workerid
	replys := <-workerReplys[workerid]
	reply.NMapTasks = replys.NMapTasks
	reply.NReduceTasks = replys.NReduceTasks
	switch replys.TaskType {
	case Map:
		reply.TaskType = Map
		reply.TaskNum = replys.TaskNum
		reply.MapFile = replys.MapFile
	case Reduce:
		reply.TaskType = Reduce
		reply.TaskNum = replys.TaskNum
	case Done:
		reply.TaskType = Done
	}
	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	workerFinisheds[args.Workid] <- args
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	mu.Lock()
	i := numWokers
	numWokers++
	workerReplys = append(workerReplys, make(chan *GetTaskReply))
	workerFinisheds = append(workerFinisheds, make(chan *FinishedTaskArgs))
	mu.Unlock()
	reply.Workerid = i
	waitWorkers <- i
	fmt.Println("register worker", i)
	return nil
}

func InWorker(i int, workers2Map chan int, workers2Red chan int) error {

	workers2Map <- i
	workers2Red <- i

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := isDone

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	workers2Map := make(chan int)
	workers2Red := make(chan int)
	mapFiles = files
	nMapTasks = len(files)
	nReduceTasks = nReduce

	go func() {
		for waitWorker := range waitWorkers {
			go InWorker(waitWorker, workers2Map, workers2Red)
		}
	}()
	// Your code here.
	go func() {
		//do map tasks issue
		DoCoordinator(workers2Map, len(files), IssueMapTask)
		DoCoordinator(workers2Red, nReduce, IssueRedTask)
		for i := 0; i < len(workerReplys); i++ {
			workerReplys[i] <- &GetTaskReply{
				TaskType: Done,
			}
		}
		isDone = true
	}()
	fmt.Println("c.server")
	c.server()
	return &c
}
