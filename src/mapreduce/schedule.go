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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// wait for all tasks being done
	wg := sync.WaitGroup{}
	wg.Add(ntasks)

	// assign tasks to workers
	for i := 0;i < ntasks;i++ {
		go func(taskId int) {
			args := DoTaskArgs{JobName: jobName, Phase: phase, TaskNumber: taskId, NumOtherPhase: n_other}
			if phase == mapPhase {
				args.File = mapFiles[taskId]
			}
			done := make(chan bool, 1)

			for {
				select {
				case workerAddr := <- registerChan:
					if call(workerAddr, "Worker.DoTask", args, nil) == true {
						wg.Done()
						registerChan <- workerAddr	// return that worker to the queue, in order to reuse it
						done <- true
					}
				default:
				}

				select {
				case <- done:
					return
				default:
					fmt.Printf("waiting for the task-%d being done\n", taskId)
				}
			}
		}(i)
	}

	wg.Wait()	// wait until all tasks are finished
	fmt.Printf("Schedule: %v done\n", phase)
}
