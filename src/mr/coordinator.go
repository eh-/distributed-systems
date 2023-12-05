package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const MaxTaskRunTime = time.Second * 10

type Coordinator struct {
	// Your definitions here.
	files []string
	reduceFiles [][]string
	fileIndexMap map[string]int
	mapTaskStates []int
	reduceTaskStates []int
	// task states: 0 (idle), 1 (in progress), 2 (complete)
	queuedMapTimes map[int]time.Time
	queuedReduceTimes map[int]time.Time
	mu sync.Mutex
}


// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	wait := false
	for mapTaskNumber, state := range c.mapTaskStates {
		if state == 0 || (state == 1 && time.Now().Sub(c.queuedMapTimes[mapTaskNumber]) > MaxTaskRunTime){
			c.mapTaskStates[mapTaskNumber] = 1
			c.queuedMapTimes[mapTaskNumber] = time.Now()
			reply.FileNames = append(reply.FileNames, c.files[mapTaskNumber])
			reply.FileNumber = mapTaskNumber
			reply.NReduce = len(c.reduceTaskStates)
			reply.Action = "map"
			return nil 
		} else if state == 1 {
			wait = true
		}
	}

	if wait {
		reply.Action = "wait"
		return nil
	}

	wait = false

	for reduceTaskNumber, state := range c.reduceTaskStates {
		if state == 0 || (state == 1 && time.Now().Sub(c.queuedReduceTimes[reduceTaskNumber]) > MaxTaskRunTime) {
			c.reduceTaskStates[reduceTaskNumber] = 1
			c.queuedReduceTimes[reduceTaskNumber] = time.Now()
			reply.FileNames = append(reply.FileNames, c.reduceFiles[reduceTaskNumber]...)
			reply.FileNumber = reduceTaskNumber
			reply.Action = "reduce"
			return nil
		} else if state == 1 {
			wait = true
		}
	} 

	if wait {
		reply.Action = "wait"
		return nil
	}
	
	reply.Action = ""
	return nil
}

func (c *Coordinator) FinishMapJob(args *FinishMapJobArgs, reply *FinishMapJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTaskStates[args.FileNumber] = 2
	delete(c.queuedMapTimes, args.FileNumber)
	for index, fileNames := range(args.ReduceFiles) {
		c.reduceFiles[index] = append(c.reduceFiles[index], fileNames...)
	}
	return nil
}

func (c *Coordinator) FinishReduceJob(args *FinishReduceJobArgs, reply *FinishReduceJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTaskStates[args.FileNumber] = 2
	delete(c.queuedReduceTimes, args.FileNumber)
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


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
func (c *Coordinator) Done() bool {
	//ret := false 
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.mapTaskStates {
		if v != 2 {
			return false
		}
	}

	for _, v := range c.reduceTaskStates {
		if v != 2 {
			return false
		}
	}
	
	// Your code here.
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fileIndexMap := make(map[string]int)
	mapTaskStates := make([]int, len(files))
	for i := 0; i < len(files); i++ {
		fileIndexMap[files[i]] = i
		mapTaskStates[i] = 0
	}

	reduceFiles := make([][]string, nReduce)
	reduceTaskStates := make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTaskStates[i] = 0
	}

	c := Coordinator{files, reduceFiles, fileIndexMap, mapTaskStates, reduceTaskStates, make(map[int]time.Time), make(map[int]time.Time), sync.Mutex{}}
	// Your code here.


	c.server()
	return &c
}
