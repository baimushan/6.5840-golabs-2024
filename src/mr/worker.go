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
	"strconv"
	"strings"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the GetJob RPC to the coordinator.

	for {
		//log.Println("call GetJob")
		reply, ok := CallGetJob()
		if !ok {
			log.Println("ok is false")
			break
		}
		if reply.FileName == "" {
			log.Println("reply.FileName is empty")
			break
		}
		//log.Println("end call GetJob")

		log.Println("get job ", reply)
		if reply.Type == WorkerTypeMap {
			MapWorker(mapf, reply)
		} else if reply.Type == WorkerTypeReduce {
			ReduceWorker(reducef, reply)
		} else {
			log.Println("unknown worker type")
		}
	}

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func MapWorker(mapf func(string, string) []KeyValue, reply JobReply) {

	filename := reply.FileName
	intermediate := []KeyValue{}
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
	intermediate = append(intermediate, kva...)
	log.Println("filename ", filename)
	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	//sort.Sort(ByKey(intermediate))

	encMap := make(map[int]*json.Encoder)
	fileMap := make(map[int]*os.File)
	for i := 0; i < reply.Nreduce; i++ {
		outFile := fmt.Sprintf("mr-%d-%d", i, reply.TaskIdx)
		ofile, _ := os.Create(outFile)
		fileMap[i] = ofile
		enc := json.NewEncoder(ofile)
		encMap[i] = enc

	}

	i := 0
	for i < len(intermediate) {
		err := encMap[ihash(intermediate[i].Key)%reply.Nreduce].Encode(&intermediate[i])
		if err != nil {
			log.Printf("Error encoding %v: %v", intermediate[i], err)
		}
		i++
	}

	for _, ofile := range fileMap {
		ofile.Sync()
		ofile.Close()
	}
	log.Println("finsh reply job", reply)

	SubmitJobStatus(JobStatusArgs{FileName: filename, Type: reply.Type})

}

func MapWorker1(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, reply JobReply) {

	filename := reply.FileName
	intermediate := []KeyValue{}
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
	intermediate = append(intermediate, kva...)
	log.Println("filename ", filename)
	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	sort.Sort(ByKey(intermediate))

	fileMap := make(map[int]*os.File)
	for i := 0; i < reply.Nreduce; i++ {
		outFile := fmt.Sprintf("mr-%d-%v", i, filepath.Base(reply.FileName))
		ofile, _ := os.Create(outFile)
		fileMap[i] = ofile
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
		fmt.Fprintf(fileMap[ihash(intermediate[i].Key)%reply.Nreduce], "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	for _, ofile := range fileMap {
		ofile.Sync()
		ofile.Close()
	}

	SubmitJobStatus(JobStatusArgs{FileName: filename, Type: reply.Type})

}

func ReduceWorker(reducef func(string, []string) string, reply JobReply) {

	intermediate := []KeyValue{}

	for _, file := range reply.ReduceFiles {
		file, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	tempFile, err := os.CreateTemp("", "example-*.txt")
	if err != nil {
		log.Fatal("无法创建临时文件:", err)
		return
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

	tempFilePath := tempFile.Name()

	if err := tempFile.Close(); err != nil {
		log.Fatal("关闭临时文件失败:", err)
		return
	}

	if err := os.Rename(tempFilePath, reply.FileName); err != nil {
		log.Fatal("重命名文件失败:", err)
		return
	}
	log.Println("finsh reply job", reply)

	SubmitJobStatus(JobStatusArgs{FileName: reply.FileName, Type: reply.Type})
}

func ReduceWorker1(reducef func(string, []string) string, reply JobReply) {
	countMap := make(map[string]int)
	for _, file := range reply.ReduceFiles {
		fileMap, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}

		content, err := ioutil.ReadAll(fileMap)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		fileMap.Close()
		kvas := strings.Split(string(content), "\n")
		for _, kva := range kvas {
			if len(kva) > 0 {
				fields := strings.Fields(kva)

				number, err := strconv.Atoi(fields[1])
				if err != nil {
					fmt.Printf("无法解析数字 '%s': %v\n", fields[1], err)
					continue
				}
				//fmt.Printf("%v %v\n", fields[0], output)
				countMap[fields[0]] += number
			}
		}
	}
	ofile, _ := os.Create(reply.FileName)
	for k, v := range countMap {
		fmt.Fprintf(ofile, "%v %d\n", k, v)
		//output := reducef(intermediate[i].Key, values)
		//fmt.Fprintf(fileMap[ihash(intermediate[i].Key)%reply.Nreduce], "%v %v\n", intermediate[i].Key, output)
	}
	ofile.Sync()
	ofile.Close()
	SubmitJobStatus(JobStatusArgs{FileName: reply.FileName, Type: reply.Type})
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallGetJob() (JobReply, bool) {

	// declare an argument structure.
	args := JobArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := JobReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.GetJob" tells the
	// receiving server that we'd like to call
	// the GetJob() method of struct Coordinator.
	//ok := call("Coordinator.GetJob", &args, &reply)
	ok := call("Coordinator.GetJob", &args, &reply)
	return reply, ok
}

func SubmitJobStatus(args JobStatusArgs) (JobStatusReply, bool) {

	// declare a reply structure.
	reply := JobStatusReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.GetJob" tells the
	// receiving server that we'd like to call
	// the GetJob() method of struct Coordinator.
	//ok := call("Coordinator.GetJob", &args, &reply)
	ok := call("Coordinator.SubmitJobStatus", &args, &reply)
	return reply, ok
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
