package iface

import (
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(parentGroupUUID, groupUUID string, meta map[string]string, taskUUIDs []string) error
	GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
	TriggerChord(groupUUID string) (bool, error)

	// Setting / getting task state
	SetStatePending(signature *tasks.Signature) error
	SetStateReceived(signature *tasks.Signature) error
	SetStateStarted(signature *tasks.Signature) error
	SetStateRetry(signature *tasks.Signature) error
	SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error
	SetStateFailure(signature *tasks.Signature, err string) error
	GetState(taskUUID string) (*tasks.TaskState, error)

	// Purging stored stored tasks states and group meta data
	IsAMQP() bool
	PurgeState(taskUUID string) error
	PurgeGroupMeta(groupUUID string) error

	CancelTask(signature *tasks.Signature) error
	UnCancelTask(signature *tasks.Signature) error
	TaskHadCanceled(taskUUID string) bool
	SetStateCanceled(signature *tasks.Signature) error
	SetStateSkipped(signature *tasks.Signature) error
	ChainTaskStates(groupUUID string) (tasks.GroupStates, error)
	ChainTasksStates(groupUUID string) (tasks.ChainTasksStates, error)
}
