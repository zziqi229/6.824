package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu   sync.Mutex
	cond *sync.Cond

	mapFinished     []bool
	reduceFinish    []bool
	mapStartTime    []time.Time
	reduceStartTime []time.Time
	NMapper         int
	NReducer        int

	MapFiles         []string
	timeoutThreshold int
	allDone          bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	// fmt.Println("call Example")
	reply.Y = args.X + 1
	return nil
}

// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) HandelGetTask(taskArgs *TaskArgs, taskReply *TaskReply) error {

	isTimeout := func(t time.Time) bool { return t.IsZero() || time.Since(t).Seconds() > float64(c.timeoutThreshold) }

	taskReply.NMapper = c.NMapper
	taskReply.NReducer = c.NReducer
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		mapDone := true
		for i, done := range c.mapFinished {
			// fmt.Printf("for i=%v\n", i)
			if !done {
				if isTimeout(c.mapStartTime[i]) {
					mapDone = false
					taskReply.TaskType = MapTask
					taskReply.TaskId = i
					taskReply.MapFile = c.MapFiles[i]
					c.mapStartTime[i] = time.Now()
					// fmt.Println("allocate map task")
					return nil
				} else {
					mapDone = false
				}
			}
		}
		if mapDone {
			break
		} else {
			c.cond.Wait()
		}
	}

	for {
		reduceDone := true
		for i, done := range c.reduceFinish {
			if !done {
				if isTimeout(c.reduceStartTime[i]) {
					taskReply.TaskType = ReduceTask
					taskReply.TaskId = i
					c.reduceStartTime[i] = time.Now()
					// fmt.Println("allocate reduce task")
					return nil
				} else {
					reduceDone = false
				}
			}
		}
		if reduceDone {
			break
		} else {
			c.cond.Wait()
		}
	}
	taskReply.TaskType = DoneTask
	c.allDone = true
	return nil
}

func (c *Coordinator) HandelFinishTask(taskArgs *TaskArgs, taskReply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch taskArgs.TaskType {
	case MapTask:
		c.mapFinished[taskArgs.TaskId] = true
		// fmt.Println("finish map task")
	case ReduceTask:
		c.reduceFinish[taskArgs.TaskId] = true
		// fmt.Println("finish reduce task")
	default:
		log.Fatalf("TaskType is wrong in HandelFinishTask")
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.timeoutThreshold = 10
	c.MapFiles = files
	c.cond = sync.NewCond(&c.mu)
	c.NMapper = len(files)
	c.NReducer = nReduce

	c.mapFinished = make([]bool, c.NMapper)
	c.reduceFinish = make([]bool, c.NReducer)

	c.mapStartTime = make([]time.Time, c.NMapper)
	c.reduceStartTime = make([]time.Time, c.NReducer)

	go func() {
		for !c.Done() {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
