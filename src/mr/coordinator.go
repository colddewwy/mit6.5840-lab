package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Task struct{
	FileName string
	Status string
	StartTime time.Time
	TaskID int
}

type Coordinator struct {
	mu sync.Mutex
	mapTasks []Task
	reduceTasks	[]Task
	nReduce	int
	mapFinished bool
	allFinished bool
	files []string
	nextTaskId int
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

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkTimeout()

	if c.allFinished{
		reply.TaskType=ExitTask
		return nil
	}
	
	if !c.mapFinished{
		for i,task := range c.mapTasks{
			if task.Status==Idle{
				reply.TaskType=MapTask
				reply.TaskID=task.TaskID
				reply.FileName=task.FileName
				reply.NReduce=c.nReduce

				c.mapTasks[i].Status=InProgress
				c.mapTasks[i].StartTime=time.Now()
				return nil
			}
		}
		reply.TaskType=WaitTask
		return nil
	}
	for i, task := range c.reduceTasks{
		if task.Status==Idle{
			reply.TaskType=ReduceTask
			reply.TaskID=task.TaskID
			reply.ReduceTaskNum=i
			reply.MapTaskNum=len(c.mapTasks)

			c.reduceTasks[i].Status=InProgress
			c.reduceTasks[i].StartTime=time.Now()
			return nil
		}
	}
	reply.TaskType=WaitTask
	return nil

}

func (c *Coordinator) checkTimeout()  {
	timeout := 10*time.Second
	now := time.Now()
	if !c.mapFinished{
		allFinished:=true
		for i,task := range c.mapTasks{
			if task.Status == InProgress && now.Sub(task.StartTime) > timeout {
				c.mapTasks[i].Status=Idle
			}
			if task.Status != Completed{
				allFinished=false
			}
		}
		c.mapFinished=allFinished
	}

	allCompleted:=true
	for i,task := range c.reduceTasks{
		if task.Status == InProgress && now.Sub(task.StartTime) > timeout {
			c.reduceTasks[i].Status=Idle
		}
		if task.Status != Completed{
			allCompleted = false
		}
	}
	c.allFinished=allCompleted

}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapTask{
		for i,task := range c.mapTasks{
			if task.TaskID==args.TaskID && task.Status == InProgress{
				task.Status = Completed
				c.mapTasks[i].Status = Completed

				allCompleted := true
				for _,task := range c.mapTasks{
					if task.Status != Completed{
						allCompleted = false
						break
					}
				}
				c.mapFinished=allCompleted
				reply.OK=true
				return nil
			}
		}
	}else if args.TaskType == ReduceTask{
		for i,task := range c.reduceTasks{
			if task.TaskID==args.TaskID && task.Status == InProgress{
				c.reduceTasks[i].Status = Completed

				allCompleted := true
				for _,task := range c.reduceTasks{
					if task.Status != Completed{
						allCompleted = false
						break
					}
				}
				c.allFinished=allCompleted
				reply.OK=true
				return nil
			}
		}
	}
	reply.OK=false
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
	ret := false

	if c.allFinished{
		ret = true
	}


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:	files,
		mapTasks:	make([]Task,len(files)),
		reduceTasks:	make([]Task,nReduce),
		nReduce:	nReduce,
		mapFinished:	false,
		allFinished:	false,
		nextTaskId:	0,
	}

	for i,file:=range c.files{
		c.mapTasks[i]=Task{
			FileName:	file,
			Status:	Idle,
			TaskID:	i,
		}
	}

	for i:=0;i<nReduce;i++{
		c.reduceTasks[i]=Task{
			TaskID : i,
			Status:	Idle,
		}
	}


	c.server()
	return &c
}
