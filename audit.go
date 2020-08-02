package orcaq

import "time"

// An AuditMessage represents something that has happened
type AuditMessage struct {
	Message string
	Time    time.Time
}
