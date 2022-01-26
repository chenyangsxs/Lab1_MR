package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//2021-11-19

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
type WorkerParameter struct {
	Mapnum    int //全部的map任务数量
	Reducenum int //全部的reduce任务数量
	WorkJob   *Job
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

//添加中间文件的生成
func (wp *WorkerParameter) DoMapWork(mapf func(string, string) []KeyValue) error {
	//一般来说一个worker执行一个文件（对于map任务来说）
	filename := wp.WorkJob.File
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	content, _ := ioutil.ReadAll(file)
	file.Close()
	kvs := mapf(filename, string(content))
	intermediate := make([][]KeyValue, wp.Reducenum, wp.Reducenum)
	for _, ky := range kvs {
		index := ihash(ky.Key) % wp.Reducenum
		intermediate[index] = append(intermediate[index], ky)
	}
	//将intermediate分发给相应的文件
	for idx := 0; idx < wp.Reducenum; idx++ {
		mapoutfilename := fmt.Sprintf("mr-%d-%d", wp.WorkJob.JobID, idx)
		//_,err = os.Stat(mapoutfilename)
		file, err = os.Create(mapoutfilename)
		data, _ := json.Marshal(intermediate[idx])
		_, err = file.Write(data)
		file.Close()
	}
	//report
	wp.ReportWork()
	return nil
}

//
func (wp *WorkerParameter) DoReduceWork(reducef func(string, []string) string) error {
	kvsReduce := make(map[string][]string)
	for idx := 0; idx < wp.Mapnum; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, wp.WorkJob.JobID)
		file, _ := os.Open(filename)
		content, _ := ioutil.ReadAll(file)
		file.Close()
		kvs := make([]KeyValue, 0)
		_ = json.Unmarshal(content, &kvs)
		for _, kv := range kvs {
			_, ok := kvsReduce[kv.Key]
			if !ok {
				kvsReduce[kv.Key] = make([]string, 0)
			}
			kvsReduce[kv.Key] = append(kvsReduce[kv.Key], kv.Value)
		}
	}
	ReduceResult := make([]string, 0)
	for key, val := range kvsReduce {
		ReduceResult = append(ReduceResult, fmt.Sprintf("%v %v\n", key, reducef(key, val)))
	}
	outFileName := fmt.Sprintf("mr-out-%d", wp.WorkJob.JobID)
	ioutil.WriteFile(outFileName, []byte(strings.Join(ReduceResult, "")), 0644)
	//report
	wp.ReportWork()
	return nil
}

//
func (wp *WorkerParameter) ReportWork() {
	//这里没有直接传送job回去，是因为害怕指针的数据被修改
	args := ReportArgs{
		JOBstate: JOBok,
		JOBtype:  wp.WorkJob.JobType,
		JOBID:    wp.WorkJob.JobID,
	}
	//delete job
	reply := ExampleReply{}
	call("Coordinator.Postonejob", &args, &reply)
	wp.WorkJob = nil
}

//
func (wp *WorkerParameter) Requestonejob() {
	// declare an argument structure.
	args := ExampleArgs{}
	reply := RequestArgs{}
	call("Coordinator.Getonejob", &args, &reply)
	wp.WorkJob = &reply.OneJob
	wp.Reducenum = reply.Reducenum
	wp.Mapnum = reply.Mapnum
}

//修改2021-11-18

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	mywp := WorkerParameter{}
	//mywp.Requestonejob()
	//should be circle
	//switch mywp.WorkJob.JOBtype {
	//case JOBtypeMap:
	//	mywp.DoMapWork(mapf)
	//case JOBtypeReduce:
	//	mywp.DoReduceWork(reducef)
	//default:
	//	// do nothing
	//}
	for {
		mywp.Requestonejob()
		time.Sleep(time.Second * 2)
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
