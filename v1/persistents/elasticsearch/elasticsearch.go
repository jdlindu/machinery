package elasticsearch

import (
	"context"
	"fmt"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/olivere/elastic"
	"time"
)

var esClient *elastic.Client

func InitClient(client *elastic.Client) {
	esClient = client
}

type KV struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type SerializeSignature struct {
	UUID                 string      `json:"uuid"`
	Name                 string      `json:"name"`
	Meta                 []KV        `json:"meta"`
	StepName             string      `json:"step_name"`
	Step                 int         `json:"step"`
	TotalStep            int         `json:"total_step"`
	RoutingKey           string      `json:"routing_key"`
	ETA                  *time.Time  `json:"eta"`
	ChainUUID            string      `json:"chain_uuid"`
	ChainCount           int         `json:"chain_count"`
	GroupUUID            string      `json:"group_uuid"`
	GroupTaskCount       int         `json:"group_task_count"`
	Args                 []tasks.Arg `json:"args"`
	Immutable            bool        `json:"-"`
	ReceivePipe          bool        `json:"receive_pipe"`
	RetryCount           int         `json:"retry_count"`
	RetryTimeout         int         `json:"retry_timeout"`
	RetriedTimes         int         `json:"retried_times"`
	ErrorExit            string      `json:"error_exit"`
	BrokerMessageGroupId string      `json:"-"`
}

func NewSerializeSignature(sign tasks.Signature) SerializeSignature {
	var metas []KV
	for k, v := range sign.Meta {
		metas = append(metas, KV{Key: k, Value: v})
	}

	serialized := SerializeSignature{
		UUID:                 sign.UUID,
		Name:                 sign.Name,
		Meta:                 metas,
		StepName:             sign.StepName,
		Step:                 sign.Step,
		TotalStep:            sign.TotalStep,
		RoutingKey:           sign.RoutingKey,
		ETA:                  sign.ETA,
		ChainUUID:            sign.ChainUUID,
		ChainCount:           sign.ChainCount,
		GroupUUID:            sign.GroupUUID,
		GroupTaskCount:       sign.GroupTaskCount,
		Args:                 sign.Args,
		Immutable:            sign.Immutable,
		ReceivePipe:          sign.ReceivePipe,
		RetryCount:           sign.RetryCount,
		RetryTimeout:         sign.RetryTimeout,
		RetriedTimes:         sign.RetriedTimes,
		ErrorExit:            sign.ErrorExit,
		BrokerMessageGroupId: sign.BrokerMessageGroupId,
	}
	return serialized
}

func serializeSignature(sign *tasks.Signature) SerializeSignature {
	return NewSerializeSignature(*sign)
}

func SaveSignature(sign *tasks.Signature) error {
	_, err := esClient.Index().
		Index("machinery-signature").
		Type("signature").
		Id(sign.UUID).
		BodyJson(serializeSignature(sign)).
		Do(context.TODO())
	return err
}

type SerializeGroup struct {
	Meta              []KV      `json:"meta"`
	ParentGroupUUID   string    `json:"parent_group_uuid"`
	GroupUUID         string    `json:"group_uuid"`
	TaskUUIDs         []string  `json:"task_uui_ds"`
	ChordTriggered    bool      `json:"-"`
	Lock              bool      `json:"-"`
	CreatedAt         time.Time `json:"created_at"`
	CurrentStepIndex  int       `json:"current_step_index"`
	CurrentStepName   string    `json:"current_step_name"`
	CurrentStepStatus string    `json:"current_step_status"`
	Status            string    `json:"status"`
	Completed         bool      `json:"completed"`
	EndAt             time.Time `json:"end_at"`
}

func NewSerializeGroup(groupMeta tasks.GroupMeta) SerializeGroup {
	var metas []KV
	for k, v := range groupMeta.Meta {
		metas = append(metas, KV{Key: k, Value: v})
	}
	serializeGroup := SerializeGroup{
		Meta:            metas,
		ParentGroupUUID: groupMeta.ParentGroupUUID,
		GroupUUID:       groupMeta.GroupUUID,
		TaskUUIDs:       groupMeta.TaskUUIDs,
		ChordTriggered:  groupMeta.ChordTriggered,
		Lock:            groupMeta.Lock,
		CreatedAt:       groupMeta.CreatedAt,
	}
	return serializeGroup
}

func serializeGroup(groupMeta *tasks.GroupMeta) SerializeGroup {
	return NewSerializeGroup(*groupMeta)
}

func SaveGroupMeta(group *tasks.GroupMeta) error {
	_, err := esClient.Index().
		Index("machinery-group").
		Type("group").
		Id(group.GroupUUID).
		BodyJson(serializeGroup(group)).
		Do(context.TODO())
	return err
}

type SerializeTaskState struct {
	SerializeSignature
	TaskUUID  string              `json:"task_uuid"`
	TaskName  string              `json:"task_name"`
	State     string              `json:"state"`
	Results   []*tasks.TaskResult `json:"results"`
	Error     string              `json:"error"`
	CreatedAt time.Time           `json:"created_at"`
	EndAt     time.Time           `json:"end_at"`
}

func NewSerializeState(state tasks.TaskState) SerializeTaskState {

	var signSerialize SerializeSignature
	if state.Signature != nil {
		signSerialize = NewSerializeSignature(*state.Signature)
	}
	serialized := SerializeTaskState{
		SerializeSignature: signSerialize,
		TaskUUID:           state.TaskUUID,
		TaskName:           state.TaskName,
		State:              state.State,
		Results:            state.Results,
		Error:              state.Error,
		CreatedAt:          state.CreatedAt,
		EndAt:              state.EndAt,
	}
	return serialized
}

func serializeTaskState(state *tasks.TaskState) SerializeTaskState {
	return NewSerializeState(*state)
}

func SaveTaskStates(state *tasks.TaskState) error {
	_, err := esClient.Index().
		Index("machinery-state").
		Type("state").
		Id(state.Signature.UUID).
		BodyJson(serializeTaskState(state)).
		Do(context.TODO())

	if err != nil {
		return err
	}

	var finalStatus string
	var completed bool
	var EndAt time.Time

	if state.IsCompleted() && len(state.Signature.OnSuccess) == 0 {
		finalStatus = state.State
		completed = true
		EndAt = time.Now()
	} else if state.IsCompleted() && !state.IsSuccess() {
		finalStatus = state.State
		completed = true
		EndAt = time.Now()
	} else {
		finalStatus = "RUNNING"
		completed = false
	}

	script := fmt.Sprintf("ctx._source.current_step_index = params.stepIndex;" +
		"ctx._source.current_step_name = params.stepName;" +
		"ctx._source.current_step_status = params.stepStatus;" +
		"ctx._source.status = params.finalStatus;" +
		"ctx._source.end_at = params.end_at;" +
		"ctx._source.completed = params.completed;")

	_, err = esClient.Update().
		Index("machinery-group").
		Type("group").
		Id(state.Signature.ChainUUID).
		Script(
			elastic.NewScript(script).Params(map[string]interface{}{
				"stepIndex":   state.Signature.Step,
				"stepName":    state.Signature.StepName,
				"stepStatus":  state.State,
				"finalStatus": finalStatus,
				"completed":   completed,
				"end_at":      EndAt,
			}).Lang("painless"),
		).Do(context.TODO())

	return err
}

type ChainTaskState struct {
	SerializeGroup
	Tasks []SerializeTaskState `json:"tasks"`
}

type GroupTaskState struct {
	SerializeGroup
	SubGroups []SerializeGroup `json:"sub_groups"`
}
