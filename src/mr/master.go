package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const defaultTimeoutInSeconds = 10 * time.Second

type Master struct {
	mapManager    *TaskManager
	reduceManager *TaskManager
	nReduce       int
}

func (m *Master) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	mapTask := m.mapManager.Trigger()
	if mapTask != nil {
		reply.TaskID = mapTask.ID
		reply.Type = TaskTypeMap
		reply.Input = mapTask.Input
		reply.NReduce = m.nReduce
		return nil
	}

	// if mapTask is nil, start reduce
	reduceTask := m.reduceManager.Trigger()
	if reduceTask != nil {
		reply.TaskID = reduceTask.ID
		reply.Type = TaskTypeReduce
		reply.Input = reduceTask.Input
		reply.NReduce = m.nReduce
		return nil
	}

	reply.Finished = true
	return nil
}

func (m *Master) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	if args == nil {
		return fmt.Errorf("args should not be nil")
	}

	if args.Type == TaskTypeMap {
		if m.mapManager.Complete(args.TaskID) {
			for idx, filepath := range args.Output {
				if filepath != "" {
					m.reduceManager.UpdateTaskInputsByID(idx, []string{filepath})
				}
			}
		}
	} else if args.Type == TaskTypeReduce {
		m.reduceManager.Complete(args.TaskID)
	} else {
		fmt.Printf("Unsupport task type %v", args.Type)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.reduceManager.Done()
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapInputs := make([][]string, len(files))
	for idx, file := range files {
		mapInputs[idx] = []string{file}
	}
	mapManager := NewTaskManager(mapInputs, defaultTimeoutInSeconds)
	reduceInputs := make([][]string, nReduce)
	reduceManager := NewTaskManager(reduceInputs, defaultTimeoutInSeconds)

	m := Master{
		mapManager:    mapManager,
		reduceManager: reduceManager,
		nReduce:       nReduce,
	}

	m.server()
	return &m
}
