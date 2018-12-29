package redis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/redsync"
	"github.com/gomodule/redigo/redis"
)

// Backend represents a Redis result backend
type Backend struct {
	common.Backend
	host     string
	password string
	db       int
	pool     *redis.Pool
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	common.RedisConnector
}

// New creates Backend instance
func New(cnf *config.Config, host, password, socketPath string, db int) iface.Backend {
	return &Backend{
		Backend:    common.NewBackend(cnf),
		host:       host,
		db:         db,
		password:   password,
		socketPath: socketPath,
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	conn := b.open()
	defer conn.Close()

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return err
	}

	return b.setExpirationTime(groupUUID)
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	for _, taskState := range taskStates {
		if !taskState.IsCompleted() {
			return false, nil
		}
	}

	return true, nil
}

func (b *Backend) ChainTasksStates(groupUUID string) (tasks.ChainTasksStates, error) {
	var tasksStates tasks.ChainTasksStates
	tasksStates.GroupUUID = groupUUID
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return tasksStates, err
	}

	for _, taskUUID := range groupMeta.TaskUUIDs {
		if !strings.HasPrefix(taskUUID, "group_") {
			return tasksStates, fmt.Errorf("not task group id, retry with instance query api")
		}
		groupStates, err := b.ChainTaskStates(taskUUID)
		if err != nil {
			return tasksStates, err
		}

		tasksStates.GroupStatesList = append(tasksStates.GroupStatesList, groupStates)
	}
	return tasksStates, nil
}

func (b *Backend) ChainTaskStates(groupUUID string) (tasks.GroupStates, error) {
	var groupStates tasks.GroupStates
	groupStates.GroupUUID = groupUUID
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return groupStates, err
	}

	firstTaskUUID := groupMeta.TaskUUIDs[0]
	if strings.HasPrefix(firstTaskUUID, "group_") {
		return groupStates, fmt.Errorf("not task instance id, retry with group task query api")
	}

	state, err := b.GetState(firstTaskUUID)
	if err != nil && err != redis.ErrNil {
		return groupStates, err
	}

	if state.Signature == nil {
		return groupStates, fmt.Errorf("state signature nil")
	}

	var signatures []*tasks.Signature
	signatures = append(signatures, state.Signature)

	getSign := func(sign *tasks.Signature) *tasks.Signature {
		if len(sign.OnSuccess) > 0 {
			return sign.OnSuccess[0]
		}
		return nil
	}

	for sign := getSign(state.Signature); sign != nil; sign = getSign(sign) {
		signatures = append(signatures, sign)
	}

	var states []*tasks.TaskState
	for idx, taskUUID := range groupMeta.TaskUUIDs {

		state, err := b.GetState(taskUUID)
		if err != nil && err != redis.ErrNil {
			return groupStates, err
		} else if err == redis.ErrNil {
			// chain task 未提交的任务
			states = append(states, tasks.NewNonExistTaskState(signatures[idx]))
		} else {
			states = append(states, state)
		}
	}
	groupStates.States = states
	return groupStates, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	conn := b.open()
	defer conn.Close()

	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *Backend) mergeNewTaskState(newState *tasks.TaskState) {
	state, err := b.GetState(newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	conn := b.open()
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", taskUUID))
	if err != nil {
		return nil, err
	}
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", taskUUID)
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", groupUUID)
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	conn := b.open()
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", groupUUID))
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))

	conn := b.open()
	defer conn.Close()

	// conn.Do requires []interface{}... can't pass []string unfortunately
	taskUUIDInterfaces := make([]interface{}, len(taskUUIDs))
	for i, taskUUID := range taskUUIDs {
		taskUUIDInterfaces[i] = interface{}(taskUUID)
	}

	reply, err := redis.Values(conn.Do("MGET", taskUUIDInterfaces...))
	if err != nil {
		return taskStates, err
	}

	for i, value := range reply {
		stateBytes, ok := value.([]byte)
		if !ok {
			return taskStates, fmt.Errorf("Expected byte array, instead got: %v", value)
		}

		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err := decoder.Decode(taskState); err != nil {
			log.ERROR.Print(err)
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *Backend) updateState(taskState *tasks.TaskState) error {
	conn := b.open()
	defer conn.Close()

	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	_, err = conn.Do("SET", taskState.TaskUUID, encoded)
	if err != nil {
		return err
	}

	return b.setExpirationTime(taskState.TaskUUID)
}

// setExpirationTime sets expiration timestamp on a stored task state
func (b *Backend) setExpirationTime(key string) error {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("EXPIREAT", key, expirationTimestamp)
	if err != nil {
		return err
	}

	return nil
}

// open returns or creates instance of Redis connection
func (b *Backend) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis)
	}
	if b.redsync == nil {
		var pools = []redsync.Pool{b.pool}
		b.redsync = redsync.New(pools)
	}
	return b.pool.Get()
}

func (b *Backend) CancelTask(signature *tasks.Signature) error {
	cancelSignatureKey := fmt.Sprintf("cancel_%s", signature.UUID)
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("SETEX", cancelSignatureKey, 60*60*24, 1)

	if err != nil {
		return err
	}

	return b.SetStateCanceled(signature)
}

func (b *Backend) UnCancelTask(signature *tasks.Signature) error {
	cancelSignatureKey := fmt.Sprintf("cancel_%s", signature.UUID)
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", cancelSignatureKey)

	if err != nil {
		return err
	}

	return b.SetStatePending(signature)
}

func (b *Backend) TaskHadCanceled(taskUUID string) bool {
	cancelSignatureKey := fmt.Sprintf("cancel_%s", taskUUID)
	conn := b.open()
	defer conn.Close()

	_, err := redis.Bytes(conn.Do("GET", cancelSignatureKey))
	if err != nil {
		return false
	}
	return true
}

func (b *Backend) SetStateCanceled(signature *tasks.Signature) error {
	taskState := tasks.NewCanceledTaskState(signature)
	return b.updateState(taskState)
}

func (b *Backend) SetStateSkipped(signature *tasks.Signature) error {
	taskState := tasks.NewSkippedTaskState(signature)
	return b.updateState(taskState)
}
