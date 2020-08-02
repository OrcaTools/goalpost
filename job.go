package orcaq

import (
	"encoding/json"
	"fmt"
	"time"
)

//A Job is one or more Task that represents work to be done by the Agent.
type Job struct {
	Label    string          // human friendly label
	Status   string          // current status of the job. Will always be overwritten.
	ID       []byte          // this will be a UUID, stored as bytes. Will always be overwritten.
	Tasks    []*Task         // the actual tasks that make up this job.
	Deadline time.Time       // Deadline is the time by when the job should be executed. Default is now.
	AuditLog []*AuditMessage // the audit log will contain the full history of the job
}

//DecodeJob decodes a gob encoded byte array into a Job struct and returns a pointer to it
func DecodeJob(b []byte) (*Job, error) {
	var j Job
	err := json.Unmarshal(b, &j)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// Bytes will encode the job to bytes
func (j *Job) Bytes() []byte {
	// TODO: handle encoding errors
	bytes, _ := json.Marshal(j)
	return bytes
}

//RecoverableWorkerError defines an error that a worker DoWork func
//can return that indicates the message should be retried
type RecoverableWorkerError struct {
	message string
}

func (e RecoverableWorkerError) Error() string {
	return fmt.Sprintf("%s", e.message)
}

//NewRecoverableWorkerError creates a new RecoverableWorkerError
func NewRecoverableWorkerError(message string) RecoverableWorkerError {
	return RecoverableWorkerError{message}
}
