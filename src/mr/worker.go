package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	for {
		args := GetJobArgs{}
		reply := GetJobReply{}
		call("Coordinator.GetJob", &args, &reply)

		if reply.Action == "map" {
			mapFiles(mapf, &reply)
		} else if reply.Action == "reduce" {
			reduceFiles(reducef, &reply)
		} else if reply.Action == "wait" {
			time.Sleep(time.Second)
		} else if reply.Action == "" {
			break
		}
	}
}

func mapFiles(mapf func(string, string) []KeyValue, reply *GetJobReply){
	nReduce := reply.NReduce
	var intermediates [][]KeyValue = make([][]KeyValue, nReduce)
	
	finishMapJobArgs := FinishMapJobArgs{reply.FileNumber, make([][]string, nReduce)}
	finishMapJobReply := FinishMapJobReply{}

	fileNames := reply.FileNames
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()

		kva := mapf(fileName, string(content))
		for _, v := range kva {
			index := ihash(v.Key) % nReduce
			intermediates[index] = append(intermediates[index], v)
		}

		for i, v := range intermediates {
			intermediateName := fmt.Sprintf("mr-%d-%d", reply.FileNumber, i)
			intermediateFile, _ := os.Create(intermediateName)
			enc := json.NewEncoder(intermediateFile)
			for _, kv := range v {
				enc.Encode(&kv)
			}
			intermediateFile.Close()
			finishMapJobArgs.ReduceFiles[i] = append(finishMapJobArgs.ReduceFiles[i], intermediateName)
		}
	}
	call("Coordinator.FinishMapJob", &finishMapJobArgs, &finishMapJobReply)
}

func reduceFiles(reducef func(string, []string) string, reply *GetJobReply){
	fileNames := reply.FileNames
	kva := make([]KeyValue, 0)
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	outputName := fmt.Sprintf("mr-out-%d", reply.FileNumber)
	ofile, _ := os.Create(outputName)

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
	
	finishReduceJobArgs := FinishReduceJobArgs{reply.FileNumber}
	finishReduceJobReply := FinishReduceJobReply{}
	call("Coordinator.FinishReduceJob", &finishReduceJobArgs, &finishReduceJobReply)
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
