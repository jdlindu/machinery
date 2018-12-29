package tasks

import "time"

const (
	// StatePending - initial state of a task
	StatePending = "PENDING"
	// StateReceived - when task is received by a worker
	StateReceived = "RECEIVED"
	// StateStarted - when the worker starts processing the task
	StateStarted = "STARTED"
	// StateRetry - when failed task has been scheduled for retry
	StateRetry = "RETRY"
	// StateSuccess - when the task is processed successfully
	StateSuccess = "SUCCESS"
	// StateFailure - when processing of the task fails
	StateFailure  = "FAILURE"
	StateCanceled = "CANCELED"
	StateSkipped  = "SKIPPED"
	// chain task not create task yet
	StateNonExist = "NONEXIST"
)

// TaskState represents a state of a task
type TaskState struct {
	Signature *Signature    `bson:"signature"`
	TaskUUID  string        `bson:"_id"`
	TaskName  string        `bson:"task_name"`
	State     string        `bson:"state"`
	Results   []*TaskResult `bson:"results"`
	Error     string        `bson:"error"`
	CreatedAt time.Time     `bson:"created_at"`
	EndAt     time.Time     `bson:"end_at"`
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
	Meta           map[string]string
	GroupUUID      string    `bson:"_id"`
	TaskUUIDs      []string  `bson:"task_uuids"`
	ChordTriggered bool      `bson:"chord_triggered"`
	Lock           bool      `bson:"lock"`
	CreatedAt      time.Time `bson:"created_at"`
}

func NewNonExistTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateNonExist,
		CreatedAt: time.Now().UTC(),
	}
}

// NewPendingTaskState ...
func NewPendingTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StatePending,
		CreatedAt: time.Now().UTC(),
	}
}

// NewReceivedTaskState ...
func NewReceivedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateReceived,
	}
}

// NewStartedTaskState ...
func NewStartedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateStarted,
	}
}

// NewSuccessTaskState ...
func NewSuccessTaskState(signature *Signature, results []*TaskResult) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateSuccess,
		Results:   results,
		EndAt:     time.Now().UTC(),
	}
}

// NewFailureTaskState ...
func NewFailureTaskState(signature *Signature, err string) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateFailure,
		Error:     err,
		EndAt:     time.Now().UTC(),
	}
}

// NewRetryTaskState ...
func NewRetryTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateRetry,
	}
}

func NewCanceledTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateCanceled,
		EndAt:     time.Now().UTC(),
	}
}

func NewSkippedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateSkipped,
		EndAt:     time.Now().UTC(),
	}
}

// IsCompleted returns true if state is SUCCESS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (taskState *TaskState) IsCompleted() bool {
	return taskState.IsSuccess() || taskState.IsFailure() || taskState.IsCanceled() || taskState.IsSkipped()
}

func (taskState *TaskState) IsNonExists() bool {
	return taskState.State == StateNonExist
}

// IsSuccess returns true if state is SUCCESS
func (taskState *TaskState) IsSuccess() bool {
	return taskState.State == StateSuccess
}

// IsFailure returns true if state is FAILURE
func (taskState *TaskState) IsFailure() bool {
	return taskState.State == StateFailure
}

func (taskState *TaskState) IsCanceled() bool {
	return taskState.State == StateCanceled
}

func (taskState *TaskState) IsSkipped() bool {
	return taskState.State == StateSkipped
}

func (taskState *TaskState) IsCancelable() bool {
	return taskState.State != StateStarted && taskState.State != StateFailure && taskState.State != StateSuccess && taskState.State != StateSkipped
}

func (taskState *TaskState) IsRetryable() bool {
	return taskState.State == StateFailure || taskState.State == StateCanceled
}
