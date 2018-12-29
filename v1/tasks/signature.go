package tasks

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Name  string      `bson:"name"`
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

// Headers represents the headers which should be used to direct the task
type Headers map[string]interface{}

// Set on Headers implements opentracing.TextMapWriter for trace propagation
func (h Headers) Set(key, val string) {
	h[key] = val
}

// ForeachKey on Headers implements opentracing.TextMapReader for trace propagation.
// It is essentially the same as the opentracing.TextMapReader implementation except
// for the added casting from interface{} to string.
func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		// Skip any non string values
		stringValue, ok := v.(string)
		if !ok {
			continue
		}

		if err := handler(k, stringValue); err != nil {
			return err
		}
	}

	return nil
}

// Signature represents a single task invocation
type Signature struct {
	UUID           string
	Name           string
	TaskName       string
	InstanceName   string
	StepName       string
	Step           int
	TotalStep      int
	RoutingKey     string
	ETA            *time.Time
	ChainUUID      string
	ChainCount     int
	GroupUUID      string
	GroupTaskCount int
	Args           []Arg
	Headers        Headers
	Immutable      bool
	ReceivePipe    bool
	RetryCount     int
	RetryTimeout   int
	RetriedTimes   int
	ErrorExit      string
	OnSuccess      []*Signature
	OnError        []*Signature
	ChordCallback  *Signature
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string
}

// NewSignature creates a new task signature
func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID := uuid.New().String()
	return &Signature{
		UUID:      fmt.Sprintf("task_%v", signatureID),
		Name:      name,
		Immutable: true,
		Args:      args,
	}, nil
}

func NewJob(name, stepName string, args ...Arg) *Signature {
	signatureID := uuid.New().String()
	return &Signature{
		UUID:      fmt.Sprintf("task_%v", signatureID),
		Name:      name,
		StepName:  stepName,
		Immutable: true,
		Args:      args,
	}
}

func NewPipeJob(name, stepName string, args ...Arg) *Signature {
	job := NewJob(name, stepName, args...)
	job.ReceivePipe = true
	return job
}

func NewIntArg(val int) Arg {
	return Arg{
		Type:  "int",
		Value: val,
	}
}

func NewStringArg(val string) Arg {
	return Arg{
		Type:  "string",
		Value: val,
	}
}
