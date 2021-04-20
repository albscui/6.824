package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := GetTask()
		if reply == nil || reply.TaskType == EXIT {
			break
		}

		if reply.TaskType == MAP {
			runMap(mapf, reply.Key, reply.TaskId, reply.NReduce)
			SetTaskComplete(MAP, reply.TaskId)
		} else if reply.TaskType == REDUCE {
			runReduce(reducef, reply.TaskId)
			SetTaskComplete(REDUCE, reply.TaskId)
		} else if reply.TaskType == EMPTY {
			// all tasks are running, so sleep for a bit
			time.Sleep(100 * time.Millisecond)
		}
	}
	log.Println("Worker exiting")
}

func runMap(mapf func(string, string) []KeyValue, filename string, mapId int, nReduce int) {
	log.Printf("Running MapTask-%v on %v", mapId, filename)

	// read entire content of file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// call UDF map
	// mapf could crash, but if it does, the following file won't be created
	kva := mapf(filename, string(content))

	// write output to intermediary files
	reduceIdToKVMap := map[int][]KeyValue{}
	for i := range kva {
		reduceId := ihash(kva[i].Key) % nReduce
		reduceIdToKVMap[reduceId] = append(reduceIdToKVMap[reduceId], kva[i])
	}
	for reduceId, kvs := range reduceIdToKVMap {
		oname := fmt.Sprintf("mr-%v-%v", mapId, reduceId)
		tmpfile, err := ioutil.TempFile(".", oname)
		if err != nil {
			log.Fatal(err)
		}
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range kvs {
			err = enc.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.Rename(tmpfile.Name(), oname); err != nil {
			log.Fatal(err)
		}
	}
}

func runReduce(reducef func(string, []string) string, reduceId int) {
	log.Printf("Running ReduceTask-%v", reduceId)
	files, err := filepath.Glob(fmt.Sprintf("mr-*-%v", reduceId))
	if err != nil {
		log.Fatalln("Cannot list reduce files")
	}

	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open file %v\n", filename)
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// sort intermediate keys and write results to output
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", reduceId)
	tmpfile, err := ioutil.TempFile(".", oname)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
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
		// reducef could crash
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Rename(tmpfile.Name(), oname); err != nil {
		log.Fatal(err)
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

func GetTask() *GetTaskReply {
	reply := GetTaskReply{}
	if call("Coordinator.GetTask", &GetTaskArgs{}, &reply) {
		return &reply
	}
	return nil
}

func SetTaskComplete(taskType TaskType, taskId int) *SetTaskCompleteReply {
	args := SetTaskCompleteArgs{taskType, taskId}
	reply := SetTaskCompleteReply{}
	if call("Coordinator.SetTaskComplete", &args, &reply) {
		return &reply
	}
	return nil
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
		log.Fatal(rpcname, "dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
