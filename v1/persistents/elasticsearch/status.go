package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"git.code.oa.com/storage-ops/golib/common/utils"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/olivere/elastic"
	"sort"
	"time"
)

func QueryGroupState(groupUUID string) (SerializeGroup, error) {
	var groupMeta SerializeGroup

	query, err := esClient.Search().
		Index("machinery-group").
		Query(elastic.NewTermQuery("_id", groupUUID)).
		Size(1).
		Pretty(true).
		Do(context.TODO())
	if err != nil {
		return groupMeta, err
	}

	if query.Hits.TotalHits < 1 {
		return groupMeta, fmt.Errorf("group uuid not exists")
	}

	source := query.Hits.Hits[0].Source

	err = json.Unmarshal(*source, &groupMeta)
	if err != nil {
		return groupMeta, err
	}

	return groupMeta, nil
}

func QueryTaskStates(taskIDs ...string) ([]SerializeTaskState, error) {
	var states []SerializeTaskState

	var taskIDList []interface{}

	for _, id := range taskIDs {
		taskIDList = append(taskIDList, id)
	}

	query, err := esClient.Search().
		Index("machinery-state").
		Query(elastic.NewTermsQuery("_id", taskIDList...)).
		Size(1000).
		Pretty(true).
		Do(context.TODO())
	if err != nil {
		return states, err
	}

	if query.Hits.TotalHits < 1 {
		return states, fmt.Errorf("task uuid not exists")
	}

	for _, hit := range query.Hits.Hits {
		var stateHit SerializeTaskState
		err = json.Unmarshal(*hit.Source, &stateHit)
		if err != nil {
			return states, err
		}
		states = append(states, stateHit)
	}
	return states, nil
}

func QueryChainTaskByID(chainUUID string) (ChainTaskState, error) {
	var groupState ChainTaskState

	groupMeta, err := QueryGroupState(chainUUID)
	if err != nil {
		return groupState, err
	}

	subtasks, err := QueryTaskStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return groupState, err
	}

	sort.Slice(subtasks, func(i, j int) bool {
		return subtasks[i].Step < subtasks[j].Step
	})

	groupState.SerializeGroup = groupMeta
	groupState.Tasks = subtasks
	return groupState, nil
}

func QueryTaskByID(groupUUID string) (GroupTaskState, error) {

	var state GroupTaskState

	var groupStates []SerializeGroup

	groupMeta, err := QueryGroupState(groupUUID)
	if err != nil {
		return state, err
	}

	if len(groupMeta.TaskUUIDs) < 1 {
		return state, fmt.Errorf("no sub group")
	}

	groupMeta.Status = tasks.StateSuccess
	groupMeta.Completed = true

	var existsFailure bool
	for _, subGroup := range groupMeta.TaskUUIDs {
		subGroupMeta, err := QueryGroupState(subGroup)
		if err != nil {
			return state, err
		}
		if !subGroupMeta.Completed {
			groupMeta.Completed = false
			groupMeta.Status = tasks.StatePending
		}

		if subGroupMeta.Status != tasks.StateSuccess {
			existsFailure = true
		}

		groupStates = append(groupStates, subGroupMeta)
	}

	if existsFailure && groupMeta.Completed {
		groupMeta.Status = tasks.StateFailure
	}

	sort.Slice(groupStates, func(i, j int) bool {
		return groupStates[i].EndAt.After(groupStates[j].EndAt)
	})

	groupMeta.EndAt = groupStates[len(groupStates)-1].EndAt

	state.SerializeGroup = groupMeta
	state.SubGroups = groupStates
	return state, nil
}

func metaQuery(params map[string]string) elastic.Query {
	bq := elastic.NewBoolQuery()
	for k, v := range params {
		bq = bq.Must(elastic.NewTermQuery("meta.key", k), elastic.NewTermQuery("meta.value", v))
	}
	return bq
}

func FilterGroupTasks(begin, end time.Time, params map[string]string) ([]SerializeGroup, error) {
	var groups []SerializeGroup
	dateQuery := elastic.NewRangeQuery("created_at").From(begin).To(end)
	metaQuery := elastic.NewNestedQuery("meta", metaQuery(params))
	bq := elastic.NewBoolQuery()
	bq = bq.Must(dateQuery)
	bq = bq.Must(metaQuery)

	d, err := bq.Source()
	utils.PrettyJson(d)

	query, err := esClient.Search().
		Index("machinery-group").
		Query(bq).
		Size(1000).
		Pretty(true).
		Do(context.TODO())
	if err != nil {
		return groups, err
	}

	if query.Hits.TotalHits < 1 {
		return groups, fmt.Errorf("no match task")
	}

	for _, hit := range query.Hits.Hits {
		var groupMeta SerializeGroup
		source := hit.Source
		err = json.Unmarshal(*source, &groupMeta)
		if err != nil {
			return groups, err
		}

		groups = append(groups, groupMeta)
	}
	return groups, nil
}

func QueryTasksByTag(begin, end time.Time, params map[string]string) ([]GroupTaskState, error) {
	var groupTaskStates []GroupTaskState

	groups, err := FilterGroupTasks(begin, end, params)
	if err != nil {
		return groupTaskStates, err
	}

	for _, g := range groups {
		g, err := QueryTaskByID(g.GroupUUID)
		if err != nil {
			return groupTaskStates, err
		}
		groupTaskStates = append(groupTaskStates, g)
	}
	return groupTaskStates, nil
}
