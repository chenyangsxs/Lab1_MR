package mr

import (
	"container/heap"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//2021-11-20

//首先应该coordinator生成所有的job，然后开始server监听所有rpc
//搞一个简单的消息队列分发任务 感应任务状态

//枚举job状态
const (
	JOBok = iota
	JOBundistribute
	JOBwaiting
	JOBtypeMap
	JOBtypeReduce
)

//Job任务
type Job struct {
	JobState int
	JobType  int
	File     string
	JobID    int
	JobTime  int64
}

//master中的协程管理
var Alldone = make(chan bool)
var MapjobAlldone = make(chan bool)

//搞一个按照工作时间储存job的堆 堆顶是最最早被分配工作的FIFO原则
type Heap_jop []*Job

func (h Heap_jop) Len() int            { return len(h) }
func (h Heap_jop) Less(i, j int) bool  { return h[i].JobTime < h[j].JobTime }
func (h Heap_jop) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *Heap_jop) Push(x interface{}) { *h = append(*h, x.(*Job)) }
func (h *Heap_jop) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

//coordinator结构体
type Coordinator struct {
	// Your definitions here.
	/* 存储所有的Job key是jobID */
	JOBall_map    map[int]*Job
	JOBheap       Heap_jop
	JOBchan_queue chan *Job
	Report_chan   chan *ReportArgs
	JOBmap_num    int
	JOBreduce_num int
	Maplock       sync.Mutex
	Heaplock      sync.Mutex
}

//
//自定义函数区
//
//感觉没啥用
func (c *Coordinator) Getjoballmap() map[int]*Job {
	c.Maplock.Lock()
	defer c.Maplock.Unlock()
	return c.JOBall_map
}

/* 生成所有job */

func (c *Coordinator) Makejob(file []string) {
	//
	for index, filename := range file {
		job := Job{
			JobState: JOBundistribute,
			JobType:  JOBtypeMap,
			File:     filename,
			JobID:    index,
			JobTime:  time.Now().Unix(), //这个时间属实没必要
		}
		c.JOBall_map[job.JobID] = &job
	}
	for i := 0; i < c.JOBreduce_num; i++ {
		job := Job{
			JobState: JOBundistribute,
			JobType:  JOBtypeReduce,
			JobID:    c.JOBmap_num + i,
			JobTime:  time.Now().Unix(), //这个时间属实没必要
		}
		c.JOBall_map[job.JobID] = &job
	}
	fmt.Printf("map jobs %d files\n ", len(file))
	fmt.Printf("reduce jobs %d files\n ", c.JOBreduce_num)
}

// Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//RPC
func (c *Coordinator) Getonejob(args *ExampleArgs, reply *RequestArgs) error {
	//deep copy job
	jobArgs := <-c.JOBchan_queue
	reply.OneJob = *jobArgs
	//reply.OneJob = <-c.JOBchan_queue
	reply.Mapnum = c.JOBmap_num
	reply.Reducenum = c.JOBreduce_num
	//reply.FileName = jobArgs.File
	fmt.Printf("worker get one job %s,chan remain %d jobs\n", reply.OneJob.File, len(c.JOBchan_queue))
	c.Addjobtoheap(jobArgs)
	return nil
}

//RPC
func (c *Coordinator) Postonejob(args *ReportArgs, reply *ExampleReply) error {
	//report信息应该阻塞
	c.Report_chan <- args
	return nil
}

//收集report
func (c *Coordinator) HandleReport() {
	for {
		reportargs := <-c.Report_chan
		c.Maplock.Lock()
		if c.JOBall_map[reportargs.JOBID].JobState == JOBwaiting {
			c.JOBall_map[reportargs.JOBID].JobState = JOBok
		}
		c.Maplock.Unlock()
		time.Sleep(time.Second)
	}
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
//重新制作了master的流程
//用一个map管理所有任务的状态 只对其加锁
//done()开启三个协程  分发map任务  分发reduce任务（需要等待上一个协程执行完毕） 检查超时任务
//worker和master用两个channel通信
//三个协程用一个channel通信
/* 检查undistribute 任务 */
func (c *Coordinator) Putjob(jobtype int) {
	//这里设置成先遍历一边map 统计未分发的任务再试图去加入ch中
	//修改map的权限只给主线程
	if jobtype == JOBtypeMap {
		//do nothing
	} else {
		//说明此协程是执行reduce任务的，需要等待map任务全部结束
		<-MapjobAlldone
	}
	for {
		finishedJobNum := 0
		var jobshouldputted *Job
		c.Maplock.Lock()
		for _, job := range c.JOBall_map {
			if job.JobType == jobtype && job.JobState == JOBundistribute {
				jobshouldputted = job
				break
			} else if job.JobType == jobtype && job.JobState == JOBok {
				finishedJobNum++
			}
		}
		c.Maplock.Unlock()
		//处理不涉及临界区的工作
		if jobshouldputted != nil {
			jobshouldputted.JobState = JOBwaiting
			c.JOBchan_queue <- jobshouldputted
			fmt.Printf("job chan has %d jobs\n", len(c.JOBchan_queue))
			//c.Addjobtoheap(jobshouldputted)
			//添加成功
		} else {
			if finishedJobNum == c.JOBmap_num && jobtype == JOBtypeMap {
				//切换到reduce
				close(MapjobAlldone)
				fmt.Printf("all map jobs done\n")
				return
				//exit
			}
			if finishedJobNum == c.JOBreduce_num && jobtype == JOBtypeReduce {
				//广播任务结束
				close(Alldone)
				fmt.Printf("all jobs done\n")
				return
				//exit
			}
		}

	}

}

//
func (c *Coordinator) Checktimeoutjob() {
	for {
		select {
		case <-Alldone:
			//exit this thread
			return
		default:
			//do nothing
		}
		c.Heaplock.Lock()
		if c.JOBheap.Len() != 0 && time.Now().Unix()-c.JOBheap[0].JobTime > 10 {
			//超时
			c.Maplock.Lock()
			c.JOBall_map[c.JOBheap[0].JobID].JobState = JOBundistribute
			fmt.Printf("one job reset\n")
			c.Maplock.Unlock()
			heap.Pop(&c.JOBheap)
		}
		c.Heaplock.Unlock()
		time.Sleep(time.Second * 2)
	}
}

//heap添加成员
func (c *Coordinator) Addjobtoheap(job *Job) {
	c.Heaplock.Lock()
	defer c.Heaplock.Unlock()
	job.JobTime = time.Now().Unix()
	heap.Push(&c.JOBheap, job)
	fmt.Printf("heap has %d job\n", c.JOBheap.Len())
}

func (c *Coordinator) Done() bool {
	ret := true
	// Your code here.
	go c.Putjob(JOBtypeMap)
	// not very elegant
	time.Sleep(time.Second * 2)
	//
	go c.Checktimeoutjob()
	<-Alldone
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JOBall_map:    make(map[int]*Job),
		JOBmap_num:    len(files),
		JOBreduce_num: nReduce,
		JOBchan_queue: make(chan *Job, 10),
		Report_chan:   make(chan *ReportArgs, 10),
	}
	heap.Init(&c.JOBheap)
	c.Makejob(files)
	// Your code here.

	c.server()
	return &c
}
