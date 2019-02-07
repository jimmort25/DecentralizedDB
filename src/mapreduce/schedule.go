package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		go func(jobNum int) {
			for {
				wk := <-mr.registerChannel

				job := DoTaskArgs{
					JobName:       mr.jobName,
					Phase:         phase,
					TaskNumber:    jobNum,
					NumOtherPhase: nios,
				}

				if phase == mapPhase {
					job.File = mr.files[jobNum]
				}

				ok := call(wk, "Worker.DoTask", &job, nil)

				if ok {
					go func() { mr.registerChannel <- wk }()
					wg.Done()
					break
				}
				go func() { mr.registerChannel <- wk }()
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
