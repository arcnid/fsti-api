package main

import (
	"bufio"
	"context"
	
	"io"
	"log"
	"os/exec"
	"sync"
	"time"
)

// Config
const (
	WorkerExecutable   = `C:\Users\seracc\Desktop\FSSand-ssdsvm06\FSTIApp.Console.exe`
	MaxParallelWorkers = 6
	WorkerTimeout      = 20 * time.Minute
)

type Job struct {
	ID   int
	Args []string
}

func main() {
	log.Println("Starting Worker Manager v0.1")

	// Create some fake jobs for testing
	jobs := []Job{
		{ID: 1, Args: []string{"parsestring", `C:\working\stat1.xlsx`, "POData", "true"}},
		{ID: 2, Args: []string{"parsestring", `C:\working\stat2.xlsx`, "POData", "true"}},
		{ID: 3, Args: []string{"parsestring", `C:\working\stat3.xlsx`, "POData", "true"}},
		{ID: 4, Args: []string{"parsestring", `C:\working\stat4.xlsx`, "POData", "true"}},
	}

	RunJobQueue(jobs)
}

func RunJobQueue(jobs []Job) {
	var wg sync.WaitGroup
	workerSem := make(chan struct{}, MaxParallelWorkers)

	for _, job := range jobs {
		wg.Add(1)

		go func(j Job) {
			defer wg.Done()

			workerSem <- struct{}{}       // acquire slot
			defer func() { <-workerSem }() // release slot

			runWorker(j)
		}(job)
	}

	wg.Wait()
}

func runWorker(job Job) {
	log.Printf("[JOB %d] Starting with args %v\n", job.ID, job.Args)

	ctx, cancel := context.WithTimeout(context.Background(), WorkerTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, WorkerExecutable, job.Args...)

	// Capture stdout and stderr
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		log.Printf("[JOB %d] Failed to start: %v", job.ID, err)
		return
	}

	pid := cmd.Process.Pid
	log.Printf("[JOB %d] Started process PID %d", job.ID, pid)

	// Stream logs
	go streamLogs(job.ID, stdout, false)
	go streamLogs(job.ID, stderr, true)

	// Wait for completion
	err := cmd.Wait()

	if ctx.Err() == context.DeadlineExceeded {
		log.Printf("[JOB %d] TIMEOUT — killing worker", job.ID)
		cmd.Process.Kill()
		return
	}

	if err != nil {
		log.Printf("[JOB %d] Worker exited with error: %v", job.ID, err)
	} else {
		log.Printf("[JOB %d] Completed successfully", job.ID)
	}
}

func streamLogs(jobID int, pipe io.ReadCloser, isError bool) {
	scanner := bufio.NewScanner(pipe)

	for scanner.Scan() {
		line := scanner.Text()
		if isError {
			log.Printf("[JOB %d][ERR] %s", jobID, line)
		} else {
			log.Printf("[JOB %d] %s", jobID, line)
		}
	}
}
