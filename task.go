package orcaq

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
)

// A Task is a single unit of work.
type Task struct {
	ID         string          // this will be a UUID that is generated at runtime.
	Label      string          // optional human friendly label
	Status     string          // current status of the task
	RetryCount int             // number of times this task has been retried
	RetryMax   int             // number of times this task can be retried before failing hard.
	Cmd        string          // the command that is to be ran
	Arg        []string        // any arguments that the command requires
	Stdout     bytes.Buffer    // results of the cmd that would be sent to stdout
	Stderr     bytes.Buffer    // results of the cmd that would be sent to stderr
	ExitCode   int             // exit code of the task
	AuditLog   []*AuditMessage // the audit log will contain all events that are associated with this task
}

// Run will attempt to execute the cmd on the task struct.
func (t *Task) Run() {
	t.ID = uuid.NewV4().String()         // ID is unique per Run
	cmd := exec.Command(t.Cmd, t.Arg...) // NOTE: is there a better way to do this than exec??

	// NOTE: we may not want the task to write to stdout or stderr
	// NOTE: this should probably be configurable.
	// TODO: add 'silent' flag option to the Run method to supress output.
	cmd.Stdout = io.MultiWriter(os.Stdout, &t.Stdout)
	cmd.Stderr = io.MultiWriter(os.Stderr, &t.Stderr)
	t.AuditLog = append(t.AuditLog, &AuditMessage{
		Message: "starting task",
		Time:    time.Now(),
	})

	err := cmd.Run()
	if err != nil {
		t.AuditLog = append(t.AuditLog, &AuditMessage{
			Message: fmt.Sprintf("task failed. Error: %v", err.Error()),
			Time:    time.Now(),
		})
		// log the error
		log.Errorf("task %v has failed. Error: %v", string(t.ID), err.Error())
	}
	t.AuditLog = append(t.AuditLog, &AuditMessage{
		Message: fmt.Sprintf("task completed"),
		Time:    time.Now(),
	})
	t.AuditLog = append(t.AuditLog, &AuditMessage{
		Message: fmt.Sprintf("stdout: %v", string(t.Stdout.Bytes())),
		Time:    time.Now(),
	})
	t.AuditLog = append(t.AuditLog, &AuditMessage{
		Message: fmt.Sprintf("stderr: %v", string(t.Stderr.Bytes())),
		Time:    time.Now(),
	})
}
