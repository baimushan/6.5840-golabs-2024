package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	filename string
	// step1 for input file
	// step2 for reduce output file

	startTime time.Time
	workType  WorkerType

	idx int // for map or reduce
}

type Coordinator struct {
	// Your definitions here.

	fileList []string
	nReduce  int
	nMap     int

	taskMutex sync.Mutex

	step            int //0 map, 1 reduce step  , 2 reduce step
	pendingTasks    []Task
	processingTasks map[string]Task
}

func (c *Coordinator) SubmitJobStatus(args *JobStatusArgs, reply *JobStatusReply) error {

	switch c.step {
	case 0: // map task
		c.taskMutex.Lock()
		if _, ok := c.processingTasks[args.FileName]; !ok {
			log.Println("任务 " + args.FileName + " 未分配")
		} else {
			delete(c.processingTasks, args.FileName)
		}
		log.Println("finish map task", args.FileName)
		if len(c.processingTasks) == 0 && len(c.pendingTasks) == 0 {
			log.Println("finish map task")
			c.step = 1

			//init reduce task
			for i := 0; i < c.nReduce; i++ {
				task := Task{
					filename:  fmt.Sprintf("mr-out-%d", i),
					startTime: time.Now(),
					workType:  WorkerTypeReduce,
					idx:       i,
				}
				log.Println("init reduce task", task)
				c.pendingTasks = append(c.pendingTasks, task)
			}
		}
		c.taskMutex.Unlock()
	case 1: // reduce task
		c.taskMutex.Lock()
		if _, ok := c.processingTasks[args.FileName]; !ok {
			log.Println("任务 " + args.FileName + " 未分配")
		} else {
			delete(c.processingTasks, args.FileName)
		}
		log.Println("finish reduce task", args.FileName)

		if len(c.processingTasks) == 0 && len(c.pendingTasks) == 0 {
			log.Println("finish reduce task")
			c.step = 2
			//finish
		}
		c.taskMutex.Unlock()
	}

	return nil
}

//const taskTimeout = 10 * time.Minute

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetJob(args *JobArgs, reply *JobReply) error {
	reply.Y = args.X + 1

	for {
		c.taskMutex.Lock()
		switch c.step {
		case 0:
			if len(c.pendingTasks) > 0 {
				task := c.pendingTasks[0]
				c.pendingTasks = c.pendingTasks[1:]
				task.startTime = time.Now()
				c.processingTasks[task.filename] = task
				reply.FileName = task.filename
				reply.Type = task.workType
				reply.Nreduce = c.nReduce
				reply.TaskIdx = task.idx
				c.taskMutex.Unlock()
				log.Println("get map job ", reply)
				return nil
			}
			c.taskMutex.Unlock()
			time.Sleep(1 * time.Second)
		case 1:
			if len(c.pendingTasks) > 0 {
				task := c.pendingTasks[0]
				task.startTime = time.Now()
				c.pendingTasks = c.pendingTasks[1:]
				c.processingTasks[task.filename] = task

				reply.Type = task.workType
				for i := 0; i < c.nMap; i++ {
					//outFile := fmt.Sprintf("mr-%d-%v", task.idx, filepath.Base(file))
					outFile := fmt.Sprintf("mr-%d-%d", task.idx, i)
					reply.ReduceFiles = append(reply.ReduceFiles, outFile)
				}
				reply.FileName = task.filename
				reply.TaskIdx = task.idx

				c.taskMutex.Unlock()
				log.Println("get reduce job ", reply)
				return nil
			}
			c.taskMutex.Unlock()
			time.Sleep(1 * time.Second)
		}
	}
}

const TaskTimeout = 10 * time.Second

func (c *Coordinator) checkTimeoutTasks() {
	for {
		time.Sleep(10 * time.Second) // 每10秒检查一次

		c.taskMutex.Lock()
		now := time.Now()
		for id, task := range c.processingTasks {
			if now.Sub(task.startTime) > TaskTimeout {
				log.Println("任务 " + task.filename + " 超时，重新分配")
				delete(c.processingTasks, id)
				task.startTime = time.Time{} // 清除分配时间
				c.pendingTasks = append(c.pendingTasks, task)
			}
		}
		c.taskMutex.Unlock()
	}
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
	log.Println("Coordinator listening on " + sockname)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	if c.step == 2 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		fileList:        files,
		nReduce:         nReduce,
		nMap:            len(files),
		step:            0,
		processingTasks: make(map[string]Task),
		pendingTasks:    make([]Task, 0),
	}
	log.Println("get job ", c)
	idx := 0
	for _, file := range files {
		c.pendingTasks = append(c.pendingTasks, Task{file, time.Now(), WorkerTypeMap, idx})
		idx = idx + 1
	}

	// Your code here.
	// we have x worker to deal with the task.
	go c.checkTimeoutTasks()
	c.server()
	return &c
}
