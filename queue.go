package orcaq

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/xujiajun/nutsdb"
)

const (
	jobsBucketName     = "jobs"     // jobs that need to be done (pending, processing, retrying)
	postJobsBucketName = "postjobs" // jobs that are done (completed, failed)
)

// Queue represents a queue
type Queue struct {
	//ID is a unique identifier for a Queue
	ID string
	//db represents a handle to a key/value store
	db *nutsdb.DB
	//notifier is a chan used to signal workers there is a job to begin working
	notifier chan []byte
	//workeres is a list of *Workers
	workers []*Worker
	//shutdownFuncs are context.CancleFuncs used to signal graceful shutdown
	shutdownFuncs []context.CancelFunc
	//wg is used to help gracefully shutdown workers
	wg *sync.WaitGroup

	//PollRate the duration to Sleep each worker before checking the queue for jobs again
	//queue for jobs again.
	//Default: 500 milliseconds
	PollRate time.Duration
}

//Init creates a connection to the internal database and initializes the Queue type
//filepath must be a valid path to a file. It cannot be shared between instances of
//a Queue. If the  file cannot be opened r/w, an error is returned.
func Init(filepath string) (*Queue, error) {
	q := &Queue{ID: filepath, PollRate: time.Duration(500 * time.Millisecond)}

	// create a new db
	// TODO: look into setting optimized nutsdb options, instead of using defaults
	opt := nutsdb.DefaultOptions
	opt.Dir = filepath
	db, err := nutsdb.Open(opt)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	q.db = db

	// Make notification channels
	c := make(chan []byte, 1000) //NOTE: a channel probably isn't the best way to handle the queue buffer
	q.notifier = c
	q.workers = make([]*Worker, 0)
	q.shutdownFuncs = make([]context.CancelFunc, 0)
	var wg sync.WaitGroup
	q.wg = &wg

	// //resume stopped jobs, clean completed, failed jobs
	// err = q.processJobs()
	// if err != nil {
	// 	log.Infof("Unable to resume jobs from bucket: %s", err)
	// }
	return q, nil
}

//Close attempts to gracefully shutdown all workers in a queue and shutdown the db connection
func (q *Queue) Close() error {
	for _, f := range q.shutdownFuncs {
		f()
	}
	q.wg.Wait()
	q.notifier = nil
	q.workers = nil
	q.shutdownFuncs = nil
	return q.db.Close()
}

//RegisterWorker registers a Worker to handle queued Jobs
func (q *Queue) RegisterWorker(w Worker) {
	baseCtx := context.Background()
	ctx, cancelFunc := context.WithCancel(baseCtx)
	q.shutdownFuncs = append(q.shutdownFuncs, cancelFunc)
	q.registerWorkerWithContext(ctx, w)
}

//registerWorkerWithContext contains the main loop for all Workers.
func (q *Queue) registerWorkerWithContext(ctx context.Context, w Worker) {
	q.workers = append(q.workers, &w)
	q.wg.Add(1)
	log.Infof("worker %s registered to queue", w.ID())
	//The big __main loop__ for workers.
	go func() {
		log.Infof("new worker coming online...")
		var jobID []byte
		for {
			// receive a notification from the queue chan
			select {
			case <-ctx.Done():
				log.Infof("received signal to shutdown. Exiting.")
				q.wg.Done()
				return
			case jobID = <-q.notifier:
				// NOTE: maybe we can show the human friendly job name if it exist?
				log.Infof("%v received job id %v", string(jobID), w.ID())
				job, err := q.GetJobByID(jobID)
				if err != nil {
					log.Errorf("unable to locate job %s. %v", string(job.ID), err.Error())
					continue
				}
				job.Status = "processing"
				job.AuditLog = append(job.AuditLog, &AuditMessage{
					Message: "job status updated to processing",
					Time:    time.Now(),
				})
				err = q.SaveJobState(job, 0)
				if err != nil {
					log.Errorf("unable to save job %s state: %s", string(job.ID), err.Error())
					continue
				}
				// Call the worker func handling this job
				err = w.Start(ctx, job)
				if err != nil {
					_, ok := err.(RecoverableWorkerError)
					if ok {
						//temporary error, retry
						log.Infof("Received temporary error: %s. Retrying...", err.Error())
						job.Status = "retrying"
						job.AuditLog = append(job.AuditLog, &AuditMessage{
							Message: fmt.Sprintf("retrying due to recoverable error: %v", err.Error()),
							Time:    time.Now(),
						})
						q.SaveJobState(job, 0)
					} else {
						job.Status = "failed"
						job.AuditLog = append(job.AuditLog, &AuditMessage{
							Message: fmt.Sprintf("failed due to critical error: %v", err.Error()),
							Time:    time.Now(),
						})
						q.SaveJobState(job, 0)
					}
				} else {
					job.Status = "complete"
					job.AuditLog = append(job.AuditLog, &AuditMessage{
						Message: "all task are complete",
						Time:    time.Now(),
					})
					q.SaveJobState(job, 0)
				}
				log.Infof("Finished processing job %v", string(jobID))
			default:
				// log.Infof("Worker: %s. No message to queue. Sleeping 500ms", w.ID())
				time.Sleep(q.PollRate)
			}
		}
	}()
}

//PushJob pushes a job to the queue and notifies workers
// Job.ID is always overwritten
func (q *Queue) PushJob(j *Job) ([]byte, error) {
	err := q.db.Update(func(tx *nutsdb.Tx) error {
		j.ID = []byte(uuid.NewV4().String())
		j.AuditLog = append(j.AuditLog, &AuditMessage{
			Message: "added to queue",
			Time:    time.Now(),
		})
		log.Infof("job %v added to the queue", string(j.ID))
		err := tx.Put(jobsBucketName, j.ID, j.Bytes(), 0) // setting this to 0 means no ttl.
		return err
	})
	if err != nil {
		log.Errorf("Unable to push job to queue: %s", err)
		return nil, err
	}
	q.notifier <- j.ID
	return j.ID, nil
}

//GetJobByID returns a pointer to a Job based on the primary key identifier id
func (q *Queue) GetJobByID(id []byte) (*Job, error) {
	var job *Job
	err := q.db.View(func(tx *nutsdb.Tx) error {
		e, err := tx.Get(jobsBucketName, id)
		if err != nil {
			return err
		}
		job, err = DecodeJob(e.Value)
		return err
	})
	return job, err
}

//SaveJobState will write the job state to disk
func (q *Queue) SaveJobState(job *Job, ttl uint32) error {
	log.Infof("saving job %v state", string(job.ID))
	err := q.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(jobsBucketName, job.ID, job.Bytes(), ttl)
	})
	if job.Status == "retrying" && err == nil {
		q.notifier <- job.ID
	}
	return err
}

// cleanJobs loops through all jobs marked as completed or failed and pushes those jobs to
// a secondary job bucket for a given retention period.
func (q *Queue) cleanJobs() error {
	return q.db.Update(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(jobsBucketName)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			job, err := DecodeJob(entry.Value)
			if err != nil {
				return err
			}
			switch job.Status {
			case "processing":
			case "retrying":
				break
			case "failed":
			case "completed":
				err := q.db.Update(func(tx *nutsdb.Tx) error {
					job.AuditLog = append(job.AuditLog, &AuditMessage{
						Message: fmt.Sprintf("moving job to postjobs bucket"),
						Time:    time.Now(),
					})
					return tx.Put(postJobsBucketName, job.ID, job.Bytes(), 86400)
				})
				if err != nil {
					log.Errorf("Unable to move job %v to postjobs bucket.", string(job.ID))
					return err
				}
				log.Infof("moved job %v from jobs to postjobs", string(job.ID))
				err = tx.Delete(jobsBucketName, job.ID)
				if err != nil {
					log.Errorf("Unable to delete job %v from queue.", string(job.ID))
					return err
				}
				log.Infof("removed job %v from queue", string(job.ID))
				break
			}
		}
		return nil
	})
}

// ListJobs will return a list of jobs within the selected queue
func (q *Queue) ListJobs(postJobs bool) ([]*Job, error) {
	bucket := jobsBucketName
	if postJobs == true {
		bucket = postJobsBucketName
	}
	var jobList []*Job
	err := q.db.View(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(bucket)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			job, err := DecodeJob(entry.Value)
			if err != nil {
				return err
			}
			jobList = append(jobList, job)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobList, nil
}
