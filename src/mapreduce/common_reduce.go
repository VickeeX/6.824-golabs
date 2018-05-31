package mapreduce

import (
	"os"
	"fmt"
	"encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvMap := make(map[string][]string)

	var file *os.File
	var err error
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		file, err = os.Open(fileName)
		if err != nil {
			fmt.Println("open file(%s) error!", fileName)
			return
		}

		var dec = json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				file.Close()
				break
			}

			key, value := kv.Key, kv.Value

			_, ok := kvMap[key]
			if ok {
				kvMap[key] = append(kvMap[key], value)
			} else {
				kvMap[key] = []string{value}
			}
		}

	}

	outfile, err := os.Create(outFile)
	outfileEncoder := json.NewEncoder(outfile)
	if err != nil {
		fmt.Println("open error in reduce 2.")
		return
	}

	for k := range kvMap {
		result := reduceF(k, kvMap[k])
		err := outfileEncoder.Encode(KeyValue{k, result})
		if err != nil {
			fmt.Println("write error in reduce.")
			return
		}
	}

	outfile.Close()

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
}
