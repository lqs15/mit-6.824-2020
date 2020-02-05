package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := fetchTask()
		if reply.Finished {
			log.Println("Worker Finished.")
			break
		}

		if reply.Type == TaskTypeMap {
			rPartitions := make([][]KeyValue, reply.NReduce)
			for _, filename := range reply.Input {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				for _, kv := range kva {
					rIndex := ihash(kv.Key) % reply.NReduce
					rPartitions[rIndex] = append(rPartitions[rIndex], kv)
				}
			}

			result := make([]string, reply.NReduce)
			for idx, part := range rPartitions {
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, idx)
				ofile, _ := os.Create(oname)
				enc := json.NewEncoder(ofile)
				for _, kv := range part {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write %v", oname)
					}
				}
				ofile.Close()
				result[idx] = oname
			}

			submitTask(reply.TaskID, reply.Type, result)
		} else if reply.Type == TaskTypeReduce {
			intermediate := []KeyValue{}
			for _, filename := range reply.Input {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				var kva []KeyValue
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
				intermediate = append(intermediate, kva...)
			}

			sort.Sort(ByKey(intermediate))

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//

			// because there maybe multiple reduce task writing to the same file
			// here we first create a tmp file, and rename to the final file
			tmpname := fmt.Sprintf("mr-tmp-%s-%d", randomString(6), reply.TaskID)
			oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			ofile, _ := os.Create(tmpname)
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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

			ofile.Close()
			if err := os.Rename(tmpname, oname); err != nil {
				log.Fatalf("cannot rename %s to %s", tmpname, oname)
			}
			os.Remove(tmpname)

			submitTask(reply.TaskID, reply.Type, []string{})
		}

		time.Sleep(1 * time.Second)
	}

}

func fetchTask() FetchTaskReply {
	args := FetchTaskArgs{}
	reply := FetchTaskReply{}

	call("Master.FetchTask", &args, &reply)

	return reply
}

func submitTask(taskID int, taskType TaskType, result []string) {
	args := SubmitTaskArgs{
		TaskID: taskID,
		Type:   taskType,
		Output: result,
	}
	reply := SubmitTaskReply{}

	call("Master.SubmitTask", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
