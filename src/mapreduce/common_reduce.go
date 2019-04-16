package mapreduce

import (
	"log"
	"encoding/json"
	"os"
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
	kv_map := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inter_file_name := reduceName(jobName, i, reduceTask)
		fd, err := os.Open(inter_file_name)
		defer fd.Close()
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(fd)
		var kv KeyValue
		for {
			readerr := dec.Decode(&kv)
			if readerr != nil {
				break
			}
			if _, ok := kv_map[kv.Key]; ok {
				kv_map[kv.Key] = append(kv_map[kv.Key], kv.Value)
			} else {
				kv_map[kv.Key] = make([]string, 0)
				kv_map[kv.Key] = append(kv_map[kv.Key], kv.Value)
			}
		}
	}
	out_fd, out_err := os.OpenFile(outFile, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
	defer out_fd.Close()
	if out_err != nil {
		log.Fatal(out_err)
	}
	enc := json.NewEncoder(out_fd)
	for key, values := range kv_map {
		if enc_err := enc.Encode(KeyValue{key, reduceF(key, values)}); enc_err != nil {
			log.Fatal(enc_err)
		}
	}
}
