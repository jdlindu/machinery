package tasks

import (
	"fmt"
	"github.com/google/uuid"
)

// Chain creates a chain of tasks to be executed one after another
type Chain struct {
	Tasks []*Signature
	Group *Group
}

// Group creates a set of tasks to be executed in parallel
type Group struct {
	Meta      map[string]string
	GroupUUID string
	Tasks     []*Signature
}

type GroupInGruop struct {
	Meta      map[string]string
	GroupUUID string
	TaskUUIDs []string
	Chains    []*Chain
}

// Chord adds an optional callback to the group to be executed
// after all tasks in the group finished
type Chord struct {
	Group    *Group
	Callback *Signature
}

// GetUUIDs returns slice of task UUIDS
func (group *Group) GetUUIDs() []string {
	taskUUIDs := make([]string, len(group.Tasks))
	for i, signature := range group.Tasks {
		taskUUIDs[i] = signature.UUID
	}
	return taskUUIDs
}

// NewChain creates a new chain of tasks to be processed one by one, passing
// results unless task signatures are set to be immutable
func NewChain(meta map[string]string, signatures ...*Signature) (*Chain, error) {
	group, err := NewChainGroup(meta, signatures...)
	if err != nil {
		return nil, fmt.Errorf("new chain group failed")
	}

	totalStep := len(signatures)
	// Auto generate task UUIDs if needed
	for idx, signature := range signatures {
		if idx == 0 && signature.ReceivePipe {
			return nil, fmt.Errorf("第一个任务不能设置为接收上一步骤入参")
		}
		if signature.UUID == "" {
			signatureID := uuid.New().String()
			signature.UUID = fmt.Sprintf("task_%v", signatureID)
		}
		signature.Step = idx + 1
		signature.TotalStep = totalStep
		signature.ChainUUID = group.GroupUUID
		signature.ChainCount = len(signatures)

	}

	for i := len(signatures) - 1; i > 0; i-- {
		if i > 0 {
			signatures[i-1].OnSuccess = []*Signature{signatures[i]}
		}
	}

	chain := &Chain{Tasks: signatures, Group: group}

	return chain, nil
}

// NewGroup creates a new group of tasks to be processed in parallel
func NewGroup(signatures ...*Signature) (*Group, error) {
	// Generate a group UUID
	groupUUID := uuid.New().String()
	groupID := fmt.Sprintf("group_%v", groupUUID)

	// Auto generate task UUIDs if needed, group tasks by common group UUID
	for _, signature := range signatures {
		if signature.UUID == "" {
			signatureID := uuid.New().String()
			signature.UUID = fmt.Sprintf("task_%v", signatureID)
		}
		signature.GroupUUID = groupID
		signature.GroupTaskCount = len(signatures)
	}

	return &Group{
		GroupUUID: groupID,
		Tasks:     signatures,
	}, nil
}

func NewChainGroup(meta map[string]string, signatures ...*Signature) (*Group, error) {
	// Generate a group UUID
	groupUUID := uuid.New().String()
	groupID := fmt.Sprintf("group_%v", groupUUID)

	return &Group{
		Meta:      meta,
		GroupUUID: groupID,
		Tasks:     signatures,
	}, nil
}

func NewChainTasks(meta map[string]string, chains ...*Chain) (*GroupInGruop, error) {
	// Generate a group UUID
	groupUUID := uuid.New().String()
	groupID := fmt.Sprintf("group_%v", groupUUID)

	var taskUUIDs []string
	for _, chain := range chains {
		taskUUIDs = append(taskUUIDs, chain.Group.GroupUUID)
	}
	return &GroupInGruop{
		Meta:      meta,
		GroupUUID: groupID,
		TaskUUIDs: taskUUIDs,
		Chains:    chains,
	}, nil
}

// NewChord creates a new chord (a group of tasks with a single callback
// to be executed after all tasks in the group has completed)
func NewChord(group *Group, callback *Signature) (*Chord, error) {
	if callback.UUID == "" {
		// Generate a UUID for the chord callback
		callbackUUID := uuid.New().String()
		callback.UUID = fmt.Sprintf("chord_%v", callbackUUID)
	}

	// Add a chord callback to all tasks
	for _, signature := range group.Tasks {
		signature.ChordCallback = callback
	}

	return &Chord{Group: group, Callback: callback}, nil
}

type GroupStates struct {
	GroupUUID  string
	Meta       map[string]string
	Signatures []*Signature
	States     []*TaskState
}

func (group *GroupStates) IsCompleted() bool {
	for _, s := range group.States {
		if !s.IsCompleted() {
			return false
		}
	}
	return true
}

func (group *GroupStates) CurrentState() (*TaskState, error) {
	if len(group.States) < 1 {
		return nil, fmt.Errorf("task non exists")
	}
	for idx, s := range group.States {
		if idx > 0 && s.IsNonExists() {
			return group.States[idx-1], nil
		}
	}
	return group.States[len(group.States)-1], nil
}

func (group *GroupStates) String() string {
	var msg string
	fmt.Sprintf("meta: %v\n", group.Meta)
	for idx, s := range group.States {
		msg += fmt.Sprintf("step:%d/%d task_id:%s job: %s, stepName: %s, state: %s\n", idx+1, s.Signature.ChainCount, s.TaskUUID, s.TaskName, s.Signature.StepName, s.State)
	}
	return msg
}

type ChainTasksStates struct {
	GroupUUID       string
	Meta            map[string]string
	GroupStatesList []GroupStates
}

func (group *ChainTasksStates) IsCompleted() bool {
	for _, chainGroup := range group.GroupStatesList {
		for _, s := range chainGroup.States {
			if !s.IsCompleted() {
				return false
			}
		}
	}
	return true
}

func (group *ChainTasksStates) CurrentState() map[string]*TaskState {
	states := make(map[string]*TaskState)
	for _, chainGroup := range group.GroupStatesList {
		currentState, err := chainGroup.CurrentState()
		if err != nil {
			continue
		}
		states[currentState.Signature.ChainUUID] = currentState
	}
	return states
}

func (group *ChainTasksStates) String() string {
	var msg string
	msg += fmt.Sprintf("task group id:%s, meta:%v \n", group.GroupUUID, group.Meta)
	for _, chainGroup := range group.GroupStatesList {
		msg += fmt.Sprintf("chainid:%s, meta:%v \n", chainGroup.GroupUUID, chainGroup.Meta)
		for idx, s := range chainGroup.States {
			msg += fmt.Sprintf("step:%d/%d task_id:%s job: %s, stepName: %s, meta: %v, state: %s, error: %s\n", idx+1, s.Signature.ChainCount, s.TaskUUID, s.TaskName, s.Signature.StepName, s.Signature.Meta, s.State, s.Error)
		}
		msg += "\n"
	}
	return msg
}
