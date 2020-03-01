// scheduler package provides a simple scheduler for concurrent execution of jobs.
//
// Copyright 2020 KaaIoT Technologies, LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"errors"
	"sync"

	"golang.org/x/sync/semaphore"
)

// Scheduler manages a dynamic pool of concurrent goroutines and dispatches Runnable jobs to them.
type Scheduler interface {
	// Schedule a job for execution, which may commence immediately, or at a later time, when a concurrent goroutine
	// becomes available. Jobs are started in FIFO fashion.
	// When the job queue is full, function returns ErrQueueFull.
	Schedule(job Runnable) error

	// Stop scheduler and accept no more Schedule() requests.
	// Completes the outstanding jobs, shuts down worker goroutines, and exits.
	// Consequent calls to Stop() will have no effect.
	Stop()
}

// Runnable interface for scheduled jobs.
type Runnable interface {
	// Run a job
	Run()
}

type scheduler struct {
	queue chan Runnable       // Queue of jobs waiting for execution
	wg    sync.WaitGroup      // Wait group for running goroutines
	sem   *semaphore.Weighted // Limits the amount of concurrently running jobs
}

// NewScheduler creates and returns a new Scheduler.
// - maxConcurrency limits the maximum allowed number of goroutines. Value 0 indicates no limit (in which case all
// incoming jobs will be run concurrently immediately).
// - maxQueueSize limits the maximum allowed number of jobs waiting execution.
// One of the return values is always nil.
func NewScheduler(maxConcurrency, maxQueueSize int) (Scheduler, error) {
	if maxConcurrency < 0 || maxQueueSize < 0 {
		return nil, errors.New("bad parameters")
	}

	s := &scheduler{queue: make(chan Runnable, maxQueueSize)}

	// Create a semaphore if the max concurrency is limited
	if maxConcurrency > 0 {
		s.sem = semaphore.NewWeighted(int64(maxConcurrency))
	}

	return s, nil
}

var ErrQueueFull = errors.New("queue full")

func (s *scheduler) Schedule(job Runnable) error {
	if !s.tryRunConcurrently(job) {
		return s.trySchedule(job)
	}

	return nil
}

func (s *scheduler) tryRunConcurrently(job Runnable) bool {
	const worker = 1

	if s.sem != nil {
		// Concurrency limited: try to acquire semaphore
		if !s.sem.TryAcquire(worker) {
			// Already at capacity
			return false
		}
	}

	s.wg.Add(worker)

	go func() {
		workerLoop(job, s.queue)
		s.wg.Done()

		if s.sem != nil {
			s.sem.Release(worker)
		}
	}()

	return true
}

func (s *scheduler) trySchedule(job Runnable) (e error) {
	defer func() {
		// Recover from a chan write panic, which may occur when the queue chan is already closed with Stop()
		if recover() != nil {
			e = errors.New("scheduler stopped")
		}
	}()

	select {
	case s.queue <- job:
		// Job scheduled for later execution: as soon as there is a runner available
	default:
		return ErrQueueFull
	}

	return nil
}

// workerLoop runs the initial job it receives and then tries to fetch more jobs from the queue.
// If there are no more jobs or the queue channel closes, workerLoop exits.
func workerLoop(job Runnable, queue chan Runnable) {
	keepRunning := true
	for keepRunning {
		job.Run()

		select {
		case job, keepRunning = <-queue:
		default:
			// No more jobs in the queue, shutdown
			return
		}
	}
}

func (s *scheduler) Stop() {
	defer func() {
		// Recover from a chan close panic, which may occur when the queue chan is already closed with Stop()
		_ = recover()
	}()

	close(s.queue)

	s.wg.Wait()
}
