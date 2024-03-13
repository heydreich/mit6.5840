package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Task struct {
	TaskType  int
	FileName  []string //
	TaskId    int
	ReduceNum int
}

type TaskMetaInfo struct {
	TaskAddr *Task
	State    int
}

type TaskMetaHolder struct {
	MetaMap       map[int]*TaskMetaInfo
	MapMetaMap    map[int]*TaskMetaInfo
	ReduceMetaMap map[int]*TaskMetaInfo
}

// tasktype
const (
	WaitingTask = iota
	MapTask
	ReduceTask
	ExitTask
)

// TaskMetaInfo state
const (
	Working = iota
	Done
)

// Coordinator info
const (
	Mapstate = iota
	Reducestate
	AllDone
)

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	State            int
	MapChan          chan *Task
	ReduceChan       chan *Task
	ReduceNum        int
	Files            []string
	MrtaskMetaHolder TaskMetaHolder
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) PullTask(args *TaskArgs, reply *Task) error {
	select {
	case re := <-c.MapChan:
		{
			*reply = *re
		}
	case re := <-c.ReduceChan:
		{
			*reply = *re
		}
	default:
		{
			reply1 := &Task{
				TaskType: WaitingTask,
				TaskId:   -1,
			}
			reply2 := &Task{
				TaskType: ExitTask,
				TaskId:   -1,
			}
			if c.State == Mapstate {
				*reply = *reply1
			}
			if c.State == Reducestate {
				*reply = *reply2
			}

		}
	}
	return nil
}

func (t *TaskMetaHolder) CheckAllTasks() bool {
	UnDoneNum, DoneNum := 0, 0

	for _, v := range t.MetaMap {
		if v.State == Working {
			UnDoneNum++
		} else if v.State == Done {
			DoneNum++
		}
	}
	if UnDoneNum == 0 && DoneNum > 0 {
		return true
	}
	return false
}

func (c *Coordinator) ToNextState() {
	if c.State == Mapstate {
		c.State = Reducestate
		fmt.Println("state changed : Reducestate")
		c.MakeReduceTasks()
	} else if c.State == Reducestate {
		c.State = AllDone
		fmt.Println("state changed : Alldone")
	}
}

func (c *Coordinator) MarkDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	meta, ok := c.MrtaskMetaHolder.MetaMap[args.TaskId]
	if ok && meta.State == Working {
		meta.State = Done
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	if c.State != AllDone {
		if c.MrtaskMetaHolder.CheckAllTasks() {
			c.ToNextState()
		}
	}

	if c.State == AllDone {
		ret = true
	}

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:      Mapstate,
		MapChan:    make(chan *Task, len(files)),
		ReduceChan: make(chan *Task, nReduce),
		ReduceNum:  nReduce,
		Files:      files,
		MrtaskMetaHolder: TaskMetaHolder{
			MetaMap:       make(map[int]*TaskMetaInfo),
			MapMetaMap:    make(map[int]*TaskMetaInfo),
			ReduceMetaMap: make(map[int]*TaskMetaInfo),
		},
	}
	c.MakeMapTasks()

	// Your code here.

	c.server()

	return &c
}

func (c *Coordinator) MakeMapTasks() {
	for id, v := range c.Files {
		task := Task{
			TaskType:  MapTask,
			FileName:  []string{v},
			TaskId:    id,
			ReduceNum: c.ReduceNum,
		}
		TaskMetaInfo := TaskMetaInfo{&task, Working}
		c.MrtaskMetaHolder.MetaMap[id] = &TaskMetaInfo
		c.MrtaskMetaHolder.MapMetaMap[id] = &TaskMetaInfo
		fmt.Println(v, "write success!")
	}
	for _, m := range c.MrtaskMetaHolder.MapMetaMap {
		maptask := m.TaskAddr
		c.MapChan <- maptask
	}
}

func (c *Coordinator) MakeReduceTasks() {
	for id := 0; id < c.ReduceNum; id++ {
		reduceid := id + len(c.Files)
		task := Task{
			TaskType:  ReduceTask,
			FileName:  c.GetRedeceTasks(id),
			TaskId:    reduceid,
			ReduceNum: c.ReduceNum,
		}

		TaskMetaInfo := TaskMetaInfo{&task, Working}
		c.MrtaskMetaHolder.MetaMap[reduceid] = &TaskMetaInfo
		c.MrtaskMetaHolder.ReduceMetaMap[reduceid] = &TaskMetaInfo
		fmt.Println(reduceid, "add Reducetask success!")
	}
	for _, m := range c.MrtaskMetaHolder.ReduceMetaMap {
		reducetask := m.TaskAddr
		c.ReduceChan <- reducetask
	}
}

func (c *Coordinator) GetRedeceTasks(id int) []string {
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
