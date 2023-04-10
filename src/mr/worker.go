package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func performMap(TaskId int, filename string, NReducer int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open the mapfile")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read the mapfile")
	}
	file.Close()
	kva := mapf(filename, string(content))
	outfiles := make([]*os.File, NReducer)
	encoders := make([]*json.Encoder, NReducer)
	for i, _ := range outfiles {
		filename := fmt.Sprintf("intermediate-%d-%d", TaskId, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Creat intermediate occure error")
		}
		outfiles[i] = file
		encoders[i] = json.NewEncoder(outfiles[i])
		defer outfiles[i].Close()
	}
	for _, kv := range kva {
		h := ihash(kv.Key) % NReducer
		encoders[h].Encode(&kv)
	}
}
func performReduce(taskID int, NMapper int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < NMapper; i++ {
		filename := fmt.Sprintf("intermediate-%d-%d", i, taskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open %s in perfoemReduce", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if dec.Decode(&kv) != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	filename := fmt.Sprintf("mr-out-%d", taskID)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Cannot create %s", filename)
	}
	defer file.Close()
	key_begin := 0
	for key_begin < len(kva) {
		key_end := key_begin
		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}
		values := []string{}
		for i := key_begin; i < key_end; i++ {
			values = append(values, kva[i].Value)
		}
		output := reducef(kva[key_begin].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[key_begin].Key, output)
		key_begin = key_end
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	for {
		taskArgs := TaskArgs{}
		taskReply := TaskReply{}
		ok := call("Coordinator.HandelGetTask", &taskArgs, &taskReply)
		if !ok {
			break
		}
		switch taskReply.TaskType {
		case MapTask:
			performMap(taskReply.TaskId, taskReply.MapFile, taskReply.NReducer, mapf)
		case ReduceTask:
			performReduce(taskReply.TaskId, taskReply.NMapper, reducef)
		case DoneTask:
			os.Exit(0)
		default:
			log.Fatalf("TaskType is wrong in Worker %v", taskReply)
		}
		finArgs := TaskArgs{
			TaskType: taskReply.TaskType,
			TaskId:   taskReply.TaskId,
		}
		finReply := TaskReply{}
		call("Coordinator.HandelFinishTask", &finArgs, &finReply)
	}

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
