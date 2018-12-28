package mapreduce

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	reduceMap := make(map[string][]string)

	// for each intermediate mapper file, read all k-v pairs from it
	for i := 0;i < nMap;i++ {
		fileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')	// read k-v pair line by line
			if err != nil {
				break
			}
			kv := KeyValue{}
			json.Unmarshal([]byte(line), &kv)
			key := kv.Key
			value := kv.Value

			// collect values by key
			if values, ok := reduceMap[key]; ok {
				reduceMap[key] = append(values, value)
			} else {
				reduceMap[key] = make([]string, 0, 0)
				reduceMap[key] = append(reduceMap[key], value)
			}
		}
		file.Close()
	}

	// sort by key
	keys := make([]string, 0, len(reduceMap))
	for k := range reduceMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// write to output file
	out, err := os.OpenFile(outFile, os.O_CREATE | os.O_RDWR, 0777)
	defer out.Close()
	if err != nil {
		panic(err)
	}
	encoder := json.NewEncoder(out)
	for _, key := range keys {
		reduceResult := reduceF(key, reduceMap[key])
		encoder.Encode(KeyValue{Key: key, Value:reduceResult})
	}
}
