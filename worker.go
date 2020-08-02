package orcaq

import "context"

//Worker represents a worker for handling Jobs
type Worker interface {
	//Start is called when a worker picks up a job from the queue
	//Context can be used for cancelling jobs early when Close
	//is called on the Queue
	Start(context.Context, *Job) error
	//ID is a semi-unique identifier for a worker.
	ID() string
}
