package machinery

import (
	"context"
	"errors"
	"fmt"
	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v1/tracing"
	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
	"sync"

	backendsiface "github.com/RichardKnop/machinery/v1/backends/iface"
	brokersiface "github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/opentracing/opentracing-go"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config            *config.Config
	registeredTasks   map[string]interface{}
	broker            brokersiface.Broker
	backend           backendsiface.Backend
	prePublishHandler func(*tasks.Signature)
}

// NewServerWithBrokerBackend ...
func NewServerWithBrokerBackend(cnf *config.Config, brokerServer brokersiface.Broker, backendServer backendsiface.Backend) *Server {
	return &Server{
		config:          cnf,
		registeredTasks: make(map[string]interface{}),
		broker:          brokerServer,
		backend:         backendServer,
	}
}

// NewServer creates Server instance
func NewServer(cnf *config.Config) (*Server, error) {
	broker, err := BrokerFactory(cnf)
	if err != nil {
		return nil, err
	}

	// Backend is optional so we ignore the error
	backend, _ := BackendFactory(cnf)

	srv := NewServerWithBrokerBackend(cnf, broker, backend)

	return srv, nil
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       "",
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (server *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       queue,
	}
}

// GetBroker returns broker
func (server *Server) GetBroker() brokersiface.Broker {
	return server.broker
}

// SetBroker sets broker
func (server *Server) SetBroker(broker brokersiface.Broker) {
	server.broker = broker
}

// GetBackend returns backend
func (server *Server) GetBackend() backendsiface.Backend {
	return server.backend
}

// SetBackend sets backend
func (server *Server) SetBackend(backend backendsiface.Backend) {
	server.backend = backend
}

// GetConfig returns connection object
func (server *Server) GetConfig() *config.Config {
	return server.config
}

// SetConfig sets config
func (server *Server) SetConfig(cnf *config.Config) {
	server.config = cnf
}

// SetPreTaskHandler Sets pre publish handler
func (server *Server) SetPreTaskHandler(handler func(*tasks.Signature)) {
	server.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
func (server *Server) RegisterTasks(namedTaskFuncs map[string]interface{}) error {
	for _, task := range namedTaskFuncs {
		if err := tasks.ValidateTask(task); err != nil {
			return err
		}
	}
	server.registeredTasks = namedTaskFuncs
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
func (server *Server) RegisterTask(name string, taskFunc interface{}) error {
	if err := tasks.ValidateTask(taskFunc); err != nil {
		return err
	}
	server.registeredTasks[name] = taskFunc
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (server *Server) IsTaskRegistered(name string) bool {
	_, ok := server.registeredTasks[name]
	return ok
}

// GetRegisteredTask returns registered task by name
func (server *Server) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := server.registeredTasks[name]
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (server *Server) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", tracing.ProducerOption(), tracing.MachineryTag)
	defer span.Finish()

	// tag the span with some info about the signature
	signature.Headers = tracing.HeadersWithSpan(signature.Headers, span)

	// Send it on to SendTask as normal
	return server.SendTask(signature)
}

// SendTask publishes a task to the default queue
func (server *Server) SendTask(signature *tasks.Signature) (*result.AsyncResult, error) {
	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %s", err)
	}

	if server.prePublishHandler != nil {
		server.prePublishHandler(signature)
	}

	if err := server.broker.Publish(signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %s", err)
	}

	return result.NewAsyncResult(signature, server.backend), nil
}

func (server *Server) GetChainTaskStatus(groupUUID string) (tasks.GroupStates, error) {
	return server.GetBackend().ChainTaskStates(groupUUID)
}

func (server *Server) GetChainTasksStatus(groupUUID string) (tasks.ChainTasksStates, error) {
	return server.GetBackend().ChainTasksStates(groupUUID)
}

func (server *Server) RetryTask(taskUUID string) error {
	state, err := server.GetBackend().GetState(taskUUID)
	if err != nil {
		return fmt.Errorf("task state not found, cannot retry")
	}

	if !state.IsRetryable() {
		return fmt.Errorf("task in state %s, cannot retry", state.State)
	}

	sign, err := server.GetBroker().GetSignature(taskUUID)
	if err != nil {
		return fmt.Errorf("task signature not found")
	}

	err = server.GetBackend().UnCancelTask(sign)
	if err != nil {
		return err
	}

	// reset retry timeout to retry immediately
	sign.RetryTimeout = 0
	_, err = server.SendTask(sign)
	return err
}

func (server *Server) RetryChainTask(groupUUID string) error {
	groupStates, err := server.GetBackend().ChainTaskStates(groupUUID)
	if err != nil {
		return err
	}
	currentState, err := groupStates.CurrentState()
	if err != nil {
		return err
	}
	return server.RetryTask(currentState.TaskUUID)
}

func (server *Server) CancelChainTask(groupUUID string) error {
	groupStates, err := server.GetBackend().ChainTaskStates(groupUUID)
	if err != nil {
		return err
	}

	// TODO: cancel pending task, 待测试
	currentState, err := groupStates.CurrentState()
	if err != nil {
		return err
	}
	fmt.Println("cancel task ", currentState.TaskUUID, currentState.State)
	return server.CancelTask(currentState.TaskUUID)
}

func (server *Server) CancelTask(taskUUID string) error {
	taskState, err := server.GetBackend().GetState(taskUUID)
	if err != nil && err != redis.ErrNil {
		return err
	} else if err == redis.ErrNil {
		signature, err := server.GetBroker().GetSignature(taskUUID)
		if err != nil {
			return fmt.Errorf("task id %s not found", taskUUID)
		}
		taskState = tasks.NewNonExistTaskState(signature)
	}
	if !taskState.IsCancelable() {
		return fmt.Errorf("task state: %s cannot cancel", taskState.State)
	}
	err = server.GetBroker().RemoveDelayedTask(taskUUID)
	if err != nil {
		return fmt.Errorf("delete task from delay queue failed")
	}
	return server.GetBackend().CancelTask(taskState.Signature)
}

func (server *Server) SkipAndContinueChainTask(groupUUID string) error {
	groupStates, err := server.GetBackend().ChainTaskStates(groupUUID)
	if err != nil {
		return err
	}

	currentState, err := groupStates.CurrentState()
	if err != nil {
		return err
	}

	if currentState.State == tasks.StateSkipped {
		return fmt.Errorf("already skipped")
	}
	fmt.Println("skip and continue chain, group: ", groupUUID, " task: ", currentState.TaskUUID)
	return server.SkipAndContinueTask(currentState.TaskUUID)
}

func (server *Server) SkipAndContinueTask(taskUUID string) error {
	signature, err := server.GetBroker().GetSignature(taskUUID)
	if err != nil {
		return err
	}

	if len(signature.OnSuccess) == 0 {
		return fmt.Errorf("task had complated, no need to skipped")
	} else if len(signature.OnSuccess) > 1 {
		return fmt.Errorf("task success callback > 1")
	}

	if err := server.GetBackend().SetStateSkipped(signature); err != nil {
		return fmt.Errorf("Set state to 'skipped' for task %s returned error: %s", signature.UUID, err)
	}

	var nextRunTask *tasks.Signature
	for _, successTask := range signature.OnSuccess {
		// 如果下一个函数以来上一个函数的结果,那么需要连下一个任务一起跳过
		if successTask.Immutable == false || successTask.ReceivePipe {
			err = server.GetBackend().SetStateSkipped(successTask)
			if err != nil {
				return fmt.Errorf("pipe task skipped failed after previous task skipped, err: %s", err.Error())
			}
			continue
		}
		nextRunTask = successTask
		break
	}

	// 剩余步骤都跳过,任务直接结束
	if nextRunTask == nil {
		return nil
	}

	_, err = server.SendTask(nextRunTask)
	return err
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChainWithContext(ctx context.Context, chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChainTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChainInfo(span, chain)

	return server.SendChain(chain)
}

func (server *Server) SendGroupChain(groupInGroup *tasks.GroupInGruop) (string, error) {

	fmt.Println(groupInGroup)
	server.backend.InitGroup(groupInGroup.GroupUUID, groupInGroup.Meta, groupInGroup.TaskUUIDs)

	for _, chain := range groupInGroup.Chains {
		_, err := server.SendChain(chain)
		if err != nil {
			return "", err
		}
	}

	return groupInGroup.GroupUUID, nil
}

// SendChain triggers a chain of tasks
func (server *Server) SendChain(chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	chainGroup := chain.Group
	server.backend.InitGroup(chainGroup.GroupUUID, chainGroup.Meta, chainGroup.GetUUIDs())
	_, err := server.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return result.NewChainAsyncResult(chain.Tasks, server.backend), nil
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowGroupTag)
	defer span.Finish()

	tracing.AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	return server.SendGroup(group, sendConcurrency)
}

// SendGroup triggers a group of parallel tasks
func (server *Server) SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	asyncResults := make([]*result.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error, len(group.Tasks)*2)

	meta := make(map[string]string)
	// Init group
	server.backend.InitGroup(group.GroupUUID, meta, group.GetUUIDs())

	// Init the tasks Pending state first
	for _, signature := range group.Tasks {
		if err := server.backend.SetStatePending(signature); err != nil {
			errorsChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {

		if sendConcurrency > 0 {
			<-pool
		}

		go func(s *tasks.Signature, index int) {
			defer wg.Done()

			// Publish task

			err := server.broker.Publish(s)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("Publish message error: %s", err)
				return
			}

			asyncResults[index] = result.NewAsyncResult(s, server.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChordTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	return server.SendChord(chord, sendConcurrency)
}

// SendChord triggers a group of parallel tasks with a callback
func (server *Server) SendChord(chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	_, err := server.SendGroup(chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return result.NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		server.backend,
	), nil
}

// GetRegisteredTaskNames returns slice of registered task names
func (server *Server) GetRegisteredTaskNames() []string {
	taskNames := make([]string, len(server.registeredTasks))
	var i = 0
	for name := range server.registeredTasks {
		taskNames[i] = name
		i++
	}
	return taskNames
}
