package elasticsearch

import (
	"context"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/olivere/elastic"
	"time"
)

var esClient *elastic.Client

func InitClient(url string) error {
	var err error
	esClient, err = elastic.NewClient(elastic.SetURL(url))
	return err
}

type KV struct {
	Key   string
	Value interface{}
}

type SerializeSignature struct {
	UUID                 string
	Name                 string
	Meta                 []KV
	StepName             string
	Step                 int
	TotalStep            int
	RoutingKey           string
	ETA                  *time.Time
	ChainUUID            string
	ChainCount           int
	GroupUUID            string
	GroupTaskCount       int
	Args                 []tasks.Arg
	Immutable            bool
	ReceivePipe          bool
	RetryCount           int
	RetryTimeout         int
	RetriedTimes         int
	ErrorExit            string
	BrokerMessageGroupId string
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
	Meta            []KV
	ParentGroupUUID string
	GroupUUID       string
	TaskUUIDs       []string
	ChordTriggered  bool
	Lock            bool
	CreatedAt       time.Time
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
	TaskUUID  string
	TaskName  string
	State     string
	Results   []*tasks.TaskResult
	Error     string
	CreatedAt time.Time
	EndAt     time.Time
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
	return err
}
