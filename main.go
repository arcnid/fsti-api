package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

//Config for Windows SSWSVM12 environment
const (
	WorkerExecutable = "C:\\Users\\seracc\\Desktop\\FSSand-ssdsvm06\\FSTIApp.Console.exe"
	MaxParallelWorkers = 6
	WorkerTimeout = 20 * time.Minute
)

type Job struct {
	ID int
	Args []string

}

func main(){
	log.PrintLn("Starting Worker Manager v0.1")

	//create some fake jobs for testing
	jobs:= []Job{
		{ID: 1, Args: []string{"parsestring", "C:\working\stat1.xlsx"}},
		{ID: 2, Args: []string{"parsestring", 'C:\working\stat2.xlsx'}},
		{ID: 3, Args: []string{"parsestring", 'C:\working\stat3.xlsx'}},
		{ID: 4, Args: []string{"parsestring", 'C:\working\stat4.xlsx'}},
	}

	RunJobQueue(jobs)
}

func RunJobQueue(jobs []Job){
	var wg sync.WaitGroup
	workerSem := make(chan struct{}, MaxParallelWorkers)

	for _, job := range jobs{
		wg.Add(1)

		go func(j Job){
			defer wg.Done()

			workerSem <- struct{}{}
			defer func() {<-workerSem}()

			runWorker(j)
		}(job)
	}

	wg.Wait()
}

func runWorker(job Job){
	log.Printf("Starting job %d with args %v", job.ID, job.Args)
	
	ctx, cancel := context.WithTimeout(context.Background(), WorkerTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, WorkerExecutable, job.Args...)

	//capture stdout
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	//start process
	if err := cmd.Start(); err != nil{
		log.Printf("Job %d failed to start: %v", job.ID, err)
		return
	}

	pid := cmd.Process.pid
	log.PrintF("[JOB %d] Started process with PID %d", job.ID, pid)

	go streamLogs(job.ID, stdout, false)

	go streamLogs(job.ID, stderr, true)


//wait for completion
	err: = cmd.Wait()

	if ctx.Err() == context.DeadlineExceeded{
		log.Printf("Job %d timed out and was killed", job.ID)
		cmd.process.Kill()
		return
	}

	if err != nil {
		log.Printf("Job %d failed: %v", job.ID, err)

	} else {
		log.Printf("Job %d completed successfully", job.ID)
	}
}

func streamLogs(jobID int, pipe io.ReadCloser, isError bool){
	scanner := bufio.NewScanner(pipe)

	for scanner.Scan(){
		line := scanner.Text()
		if isError{
			log.Printf("[JOB %d][ERR] %s", jobID, line)
		} else {
			log.Printf("[JOB %d] %s", jobID, line)
		}
	}
}