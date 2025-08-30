package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
import "sort"
import "io"
import "os"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := os.Getpid()
	for{
		task := getTask(workerID)
		switch task.TaskType{
		case MapTask:
			doMap(task,mapf,workerID)
		case ReduceTask:
			doReduce(task,reducef,workerID)
		case WaitTask:
			time.Sleep(500*time.Millisecond)
			continue
		case ExitTask:
			return
		}
	}

}

func doMap(task GetTaskReply,mapf func(string,string) []KeyValue,workerID int){
	filename := task.FileName
	file ,err := os.Open(filename)
	if err != nil{
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil{
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename,string(content))

	intermediate := make([][]KeyValue,task.NReduce)
	for _,kv := range kva {
		bucket := ihash(kv.Key)%task.NReduce
		intermediate[bucket]=append(intermediate[bucket],kv)
	}

	for i:=0;i<task.NReduce;i++{
		tempFile,err := os.CreateTemp("","mr-tmp-*")
		if err != nil{
			log.Fatalf("cannot create temp file")
		}

		enc := json.NewEncoder(tempFile)
		for _,kv := range intermediate[i]{
			err := enc.Encode(&kv)
			if err != nil{
				log.Fatalf("cannot encode %v", kv)
			}
		}
		tempFile.Close()

		os.Rename(tempFile.Name(),fmt.Sprintf("mr-%d-%d",task.TaskID,i))
	}
	reportTaskDone(task.TaskType,task.TaskID,workerID)
}

func doReduce(task GetTaskReply,reducef func(string,[]string) string,workerID int){
	reduceTaskNum :=task.ReduceTaskNum
	mapTaskNum := task.MapTaskNum
	intermediate := []KeyValue{}
	for i:=0;i<mapTaskNum;i++{
		filename := fmt.Sprintf("mr-%d-%d",i,reduceTaskNum)
		file ,err := os.Open(filename)
		if err != nil{
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
     			break
    		}
    		intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	tempFile,err := os.CreateTemp("","mr-out-tmp-*")
	if err != nil{
		log.Fatalf("cannot create temp file")
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(),fmt.Sprintf("mr-out-%d",reduceTaskNum))
	reportTaskDone(ReduceTask,task.TaskID,workerID)
}


func getTask(workerID int) GetTaskReply{
	args := GetTaskArgs{WorkerID : workerID}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}


func reportTaskDone(taskType string,taskID int,workerID int){
	args := ReportTaskArgs{TaskType:taskType, TaskID:taskID, WorkerID:workerID,Completed:true}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
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
