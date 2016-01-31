package events

import (
	"time"
)

const (
	PRIORITY_LOW = iota
	PRIORITY_HIGH
	PRIORITY_IMMEDIATE
)

const MAX_EVENT_LIFETIME = time.Minute * 5

type Event struct {
	Payload   interface{}
	Type      string
	CreatedOn time.Time
	Priority  int
}
