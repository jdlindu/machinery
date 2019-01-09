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
	StateNotCreated = "NOTCREATED"
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
	Meta            map[string]string
	ParentGroupUUID string   `bson:"parent_group_uuid"`
	GroupUUID       string   `bson:"_id"`
	TaskUUIDs       []string `bson:"task_uuids"`
	//PENDINGTasks    []string  `json:"PendingTasks"`
	//RECEIVEDTasks   []string  `json:"ReceivedTasks"`
	//STARTEDTasks    []string  `json:"StartedTasks"`
	//RETRYTasks      []string  `json:"RetryTasks"`
	//SUCCESSTasks    []string  `json:"SuccessTasks"`
	//FAILURETasks    []string  `json:"FailureTasks"`
	//CANCELEDTasks   []string  `json:"CanceledTasks"`
	//SKIPPEDTasks    []string  `json:"SkippedTasks"`
	//NOTCREATEDTasks []string  `json:"NotCreatedTasks"`
	TotalStep        int       `bson:"total_task"`     // 总步骤数
	CurrentStepIndex int       `bson:"current_task"`   // 当前步骤数
	CurrentStep      int       `bson:"current_task"`   // 当前步骤名
	CurrentStatus    string    `bson:"current_status"` // 任务步骤状态
	Completed        bool      `bson:"completed"`      // 是否已完成
	Status           string    `bson:"status"`         // 任务状态
	ChordTriggered   bool      `bson:"chord_triggered"`
	Lock             bool      `bson:"lock"`
	CreatedAt        time.Time `bson:"created_at"`
	CompletedAt      time.Time `bson:"completed_at"`
}

func NewNotCreatedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateNotCreated,
		CreatedAt: time.Now(),
	}
}

// NewPendingTaskState ...
func NewPendingTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StatePending,
		CreatedAt: time.Now(),
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
		EndAt:     time.Now(),
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
		EndAt:     time.Now(),
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
		EndAt:     time.Now(),
	}
}

func NewSkippedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		Signature: signature,
		TaskName:  signature.Name,
		State:     StateSkipped,
		EndAt:     time.Now(),
	}
}

// IsCompleted returns true if state is SUCCESS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (taskState *TaskState) IsCompleted() bool {
	return taskState.IsSuccess() || taskState.IsFailure() || taskState.IsCanceled() || taskState.IsSkipped()
}

func (taskState *TaskState) IsNotCreated() bool {
	return taskState.State == StateNotCreated
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
