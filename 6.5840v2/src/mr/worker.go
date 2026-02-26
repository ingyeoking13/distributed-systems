package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		args := JobArgs{}
		reply := JobReply{}
		ok := call("Coordinator.GetJob", &args, &reply)
		if ok {
			jobType := reply.JobType
			switch jobType {
			case MapType:
				intermediate := make([][]KeyValue, reply.NReduce)

				file, _ := os.Open(reply.FileName)
				content, _ := io.ReadAll(file)
				file.Close()
				kva := mapf(reply.FileName, string(content))

				for _, kv := range kva {
					idx := ihash(kv.Key) % reply.NReduce
					intermediate[idx] = append(intermediate[idx], kv)
				}

				for i := 0; i < len(intermediate); i++ {

					resultFileName := fmt.Sprintf("mr-inter-%d-%d", reply.Idx, i)
					fout, _ := os.Create(resultFileName)
					enc := json.NewEncoder(fout)

					for _, kv := range intermediate[i] {
						enc.Encode(&kv)
					}

					fout.Close()
				}

				finArgs := FinJobArgs{}
				finReply := FinJobReply{}

				finArgs.Idx = reply.Idx
				finArgs.JobType = MapType
				finArgs.OriFileNames = []string{reply.FileName}
				for i := 0; i < len(intermediate); i++ {
					resultFileName := fmt.Sprintf("mr-inter-%d-%d", reply.Idx, i)
					finArgs.ResFileNames = append(finArgs.ResFileNames, resultFileName)
				}

				call("Coordinator.FinJob", &finArgs, &finReply)

			case ReduceType:
				var kva []KeyValue
				for _, fileName := range reply.IntermediateFiles {
					file, _ := os.Open(fileName)
					dec := json.NewDecoder(file)

					for {
						var kv KeyValue
						err := dec.Decode(&kv)
						if err != nil {
							break
						}
						kva = append(kva, kv)
					}
					file.Close()

				}
				sort.Slice(kva, func(a, b int) bool {
					return kva[a].Key < kva[b].Key
				})

				resultFileName := fmt.Sprintf("mr-out-%d", reply.Idx)
				ofile, _ := os.Create(resultFileName)
				// repeat of mrsequential.go

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

				finArgs := FinJobArgs{}
				finReply := FinJobReply{}
				finArgs.JobType = ReduceType
				finArgs.Idx = reply.Idx
				call("Coordinator.FinJob", &finArgs, &finReply)
			case FIN:
				return
			case WAIT:
				time.Sleep(200 * time.Millisecond)
				continue
			}

		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
