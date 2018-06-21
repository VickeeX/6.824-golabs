package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	taskChan := make(chan int)
	go func() {
		for t := 0; t < ntasks; t++ {
			wg.Add(1)
			taskChan <- t
		}
		wg.Wait()
		close(taskChan)
	}()

	for task := range taskChan{
		addr := <-registerChan
		go func(j int, addr string) {
			result := call(addr, "Worker.DoTask", DoTaskArgs{jobName, mapFiles[j], phase, j, n_other}, nil)
			fmt.Println("46 current: ", j)
			if result {
				wg.Done()
			} else {
				taskChan <- j
			}
			registerChan <- addr
		}(task , addr)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
