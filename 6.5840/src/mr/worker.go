package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Your worker implementation here.
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.JobRequest", &args, &reply)
		if !ok {
			return
		}

		switch reply.TaskType {
		case MapTask: // map 동작
			// 입력 파일 읽기
			content, _ := os.ReadFile(reply.File)

			// map 함수 실행
			// Map
			kva := mapf(reply.File, string(content))
			// intermediate 파일 생성
			buckets := make([][]KeyValue, reply.NReduce)
			for _, kv := range kva {
				r := ihash(kv.Key) % reply.NReduce
				buckets[r] = append(buckets[r], kv)
			}

			for r := 0; r < reply.NReduce; r++ {
				oname := fmt.Sprintf("mr-%d-%d", reply.Id, r)
				tmp, _ := os.CreateTemp("", "mr-map-tmp-*")
				enc := json.NewEncoder(tmp)
				for _, kv := range buckets[r] {
					enc.Encode(&kv)
				}
				tmp.Close()
				os.Rename(tmp.Name(), oname)
			}

			// 완료 보고
			report := TaskArgs{
				TaskType: MapTask,
				Done:     true,
				Id:       reply.Id,
			}
			call("Coordinator.RPCDone", &report, &TaskReply{})
		case ReduceTask:
			kva := []KeyValue{}
			for m := 0; ; m++ {
				name := fmt.Sprintf("mr-%d-%d", m, reply.Id)
				file, err := os.Open(name)
				if err != nil {
					break
				}
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

			// key 정렬
			sort.Slice(kva, func(i, j int) bool {
				return kva[i].Key < kva[j].Key
			})
			// 3 reduce 실행
			oname := fmt.Sprintf("mr-out-%d", reply.Id)
			tmp, _ := os.CreateTemp("", "mr-out-tmp-*")

			for i := 0; i < len(kva); {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)
				i = j
			}
			tmp.Close()
			os.Rename(tmp.Name(), oname)

			report := TaskArgs{
				TaskType: ReduceTask,
				Id:       reply.Id,
				Done:     true,
			}
			call("Coordinator.RPCDone", &report, &TaskReply{})
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			return
		}
	}

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
