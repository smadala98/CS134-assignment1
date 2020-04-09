package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func RunParallelJob(worker string, operation JobType, jobNum int, numOtherPhase int, c chan string, mr *MapReduce) {
	args := &DoJobArgs{}
	args.Operation = operation
	args.JobNumber = jobNum
	args.NumOtherPhase = numOtherPhase
	args.File = "824-mrinput.txt"
	reply := &DoJobReply{}
	//fmt.Printf("Worker %v performing %v %v\n", worker, args.Operation, args.JobNumber)
	if !call(worker, "Worker.DoJob", args, &reply) {
		//fmt.Printf("Worker %v failed", worker)
		delete(mr.Workers, worker)
		worker = <-c
		go RunParallelJob(worker, operation, jobNum, numOtherPhase, c, mr)
	} else {
		//fmt.Printf("Worker %v completed %v %v\n", worker, args.Operation, args.JobNumber)
		c <- worker
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	fmt.Println("Running Master")

	mr.Workers = make(map[string]*WorkerInfo)

	workerChannel := make(chan string)

	mapWorkerCount := 0
	for {
		if mapWorkerCount == mr.nMap {
			break
		}
		select {
		case worker := <-mr.registerChannel:
			mr.Workers[worker] = &WorkerInfo{worker}
			go RunParallelJob(worker, "Map", mapWorkerCount, mr.nReduce, workerChannel, mr)
			mapWorkerCount++
		case oldWorker := <-workerChannel:
			go RunParallelJob(oldWorker, "Map", mapWorkerCount, mr.nReduce, workerChannel, mr)
			mapWorkerCount++
			//default:
			//fmt.Println("No workers available for map")
		}
	}

	for i := 0; i < len(mr.Workers); i++ {
		<-workerChannel
		//worker := <-workerChannel
		//fmt.Printf("Worker %v Finished Map\n", worker)
	}

	workerIndex := 0
	for worker := range mr.Workers {
		go RunParallelJob(worker, "Reduce", workerIndex, mr.nMap, workerChannel, mr)
		workerIndex++
	}

	reduceWorkerCount := 0
	for {
		if reduceWorkerCount == mr.nReduce {
			break
		}
		select {
		case worker := <-mr.registerChannel:
			mr.Workers[worker] = &WorkerInfo{worker}
			go RunParallelJob(worker, "Reduce", reduceWorkerCount, mr.nMap, workerChannel, mr)
			reduceWorkerCount++
		case oldWorker := <-workerChannel:
			go RunParallelJob(oldWorker, "Reduce", reduceWorkerCount, mr.nMap, workerChannel, mr)
			reduceWorkerCount++
			//default:
			//fmt.Println("No workers available for reduce")
		}
	}

	for i := 0; i < len(mr.Workers); i++ {
		<-workerChannel
		//worker := <-workerChannel
		//fmt.Printf("Worker %v Finished Reduce\n", worker)
	}
	return mr.KillWorkers()
}
