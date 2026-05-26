package internal

// All code in this file is private to the package.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/internal/common/retry"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/proxy"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/internal/common/serializer"
	"go.temporal.io/sdk/internal/extstore"
	"go.temporal.io/sdk/log"
)

const (
	// Server returns empty task after dynamicconfig.MatchingLongPollExpirationInterval (default is 60 seconds).
	// pollTaskServiceTimeOut should be dynamicconfig.MatchingLongPollExpirationInterval + some delta for full round trip to matching
	// because empty task should be returned before timeout is expired (expired timeout counts against SLO).
	pollTaskServiceTimeOut = 70 * time.Second

	stickyWorkflowTaskScheduleToStartTimeoutSeconds = 5

	ratioToForceCompleteWorkflowTaskComplete = 0.8
)

type workflowTaskPollerMode int

const (
	Mixed workflowTaskPollerMode = iota
	NonSticky
	Sticky
)

type (
	// taskPoller interface to poll for tasks
	taskPoller interface {
		// PollTask polls for one new task
		PollTask() (taskForWorker, error)
	}

	// taskProcessor interface to process tasks
	taskProcessor interface {
		// ProcessTask processes a task
		ProcessTask(interface{}) error
	}

	pollerScaleDecision struct {
		pollRequestDeltaSuggestion int
	}

	taskForWorker interface {
		scaleDecision() (pollerScaleDecision, bool)
		isEmpty() bool
	}

	// basePoller is the base class for all poller implementations
	basePoller struct {
		metricsHandler metrics.Handler // base metric handler used for rpc calls
		stopC          <-chan struct{}
		// The worker's build ID, either as defined by the user or automatically set
		workerBuildID string
		// Whether the worker has opted in to the build-id based versioning feature
		useBuildIDVersioning bool
		// The worker's deployment version identifier.
		workerDeploymentVersion WorkerDeploymentVersion
		// Server's capabilities
		capabilities *workflowservice.GetSystemInfoResponse_Capabilities
		// tracks timestamp for last poll request, for worker heartbeating
		pollTimeTracker *pollTimeTracker
		// Unique identifier for worker
		workerInstanceKey string
		// Server cancels polls on shutdown
		workerPollCompleteOnShutdown *atomic.Bool
	}

	// numPollerMetric tracks the number of active pollers and publishes a metric on it.
	numPollerMetric struct {
		lock       sync.Mutex
		numPollers int32
		gauge      metrics.Gauge
	}

	workflowTaskPoller struct {
		basePoller
		mode             workflowTaskPollerMode
		namespace        string
		taskQueueName    string
		identity         string
		service          workflowservice.WorkflowServiceClient
		taskHandler      WorkflowTaskHandler
		contextManager   WorkflowContextManager
		logger           log.Logger
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter

		stickyUUID                   string
		StickyScheduleToStartTimeout time.Duration

		pendingRegularPollCount int
		pendingStickyPollCount  int
		stickyBacklog           int64
		requestLock             sync.Mutex
		stickyCacheSize         int
		eagerActivityExecutor   *eagerActivityExecutor

		numNormalPollerMetric *numPollerMetric
		numStickyPollerMetric *numPollerMetric

		inboundPayloadVisitor     PayloadVisitor
		payloadVisitorConcurrency int
	}

	// workflowTaskProcessor implements processing of a workflow task and can create
	// workflow task pollers
	workflowTaskProcessor struct {
		basePoller
		namespace        string
		taskQueueName    string
		identity         string
		service          workflowservice.WorkflowServiceClient
		taskHandler      WorkflowTaskHandler
		contextManager   WorkflowContextManager
		logger           log.Logger
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter

		stickyUUID                   string
		StickyScheduleToStartTimeout time.Duration

		pendingRegularPollCount int
		pendingStickyPollCount  int
		stickyBacklog           int64
		stickyCacheSize         int
		eagerActivityExecutor   *eagerActivityExecutor

		numNormalPollerMetric *numPollerMetric
		numStickyPollerMetric *numPollerMetric

		inboundPayloadVisitor     PayloadVisitor
		outboundPayloadVisitor    PayloadVisitor
		payloadVisitorConcurrency int
	}

	// activityTaskPoller implements polling/processing a workflow task
	activityTaskPoller struct {
		basePoller
		namespace           string
		taskQueueName       string
		identity            string
		service             workflowservice.WorkflowServiceClient
		taskHandler         ActivityTaskHandler
		logger              log.Logger
		activitiesPerSecond float64
		numPollerMetric     *numPollerMetric
	}

	historyIteratorImpl struct {
		iteratorFunc  func(nextPageToken []byte) (*historypb.History, []byte, error)
		execution     *commonpb.WorkflowExecution
		nextPageToken []byte
		namespace     string
		service       workflowservice.WorkflowServiceClient
		// maxEventID is the maximum eventID that the history iterator is expected to return.
		// 0 means that the iterator will return all history events.
		maxEventID     int64
		metricsHandler metrics.Handler
		taskQueue      string
	}

	// retrievingHistoryIterator wraps a HistoryIterator and applies the inbound
	// payload visitor to each page fetched, resolving external storage references
	// in paginated history events that were not part of the initial poll response.
	retrievingHistoryIterator struct {
		inner                     HistoryIterator
		inboundVisitor            PayloadVisitor
		payloadVisitorConcurrency int
	}

	localActivityTaskPoller struct {
		basePoller
		handler      *localActivityTaskHandler
		logger       log.Logger
		laTunnel     *localActivityTunnel
		workerStopCh <-chan struct{}
	}

	localActivityTaskHandler struct {
		backgroundContext  context.Context
		metricsHandler     metrics.Handler
		logger             log.Logger
		dataConverter      converter.DataConverter
		contextPropagators []ContextPropagator
		interceptors       []WorkerInterceptor
		client             *WorkflowClient
		workerStopChannel  <-chan struct{}
	}

	localActivityResult struct {
		result  *commonpb.Payloads
		err     error
		task    *localActivityTask
		backoff time.Duration
	}

	localActivityTunnel struct {
		taskCh   chan *localActivityTask
		resultCh chan eagerOrPolledTask
		stopCh   <-chan struct{}
	}
)

func newNumPollerMetric(metricsHandler metrics.Handler, pollerType string) *numPollerMetric {
	if heartbeatHandler, isHeartbeat := metricsHandler.(*heartbeatMetricsHandler); isHeartbeat {
		metricsHandler = heartbeatHandler.forPoller(pollerType)
	}
	return &numPollerMetric{
		gauge: metricsHandler.WithTags(metrics.PollerTags(pollerType)).Gauge(metrics.NumPoller),
	}
}

func (npm *numPollerMetric) increment() {
	npm.lock.Lock()
	defer npm.lock.Unlock()
	npm.numPollers += 1
	npm.gauge.Update(float64(npm.numPollers))
}

func (npm *numPollerMetric) decrement() {
	npm.lock.Lock()
	defer npm.lock.Unlock()
	npm.numPollers -= 1
	npm.gauge.Update(float64(npm.numPollers))
}

func newLocalActivityTunnel(stopCh <-chan struct{}) *localActivityTunnel {
	return &localActivityTunnel{
		taskCh:   make(chan *localActivityTask, 100000),
		resultCh: make(chan eagerOrPolledTask),
		stopCh:   stopCh,
	}
}

func (lat *localActivityTunnel) getTask() *localActivityTask {
	select {
	case task := <-lat.taskCh:
		return task
	case <-lat.stopCh:
		return nil
	}
}

func (lat *localActivityTunnel) sendTask(task *localActivityTask) bool {
	select {
	case lat.taskCh <- task:
		return true
	case <-lat.stopCh:
		return false
	}
}

func isClientSideError(err error) bool {
	// If an activity execution exceeds deadline.
	return err == context.DeadlineExceeded
}

// stopping returns true if worker is stopping right now
func (bp *basePoller) stopping() bool {
	select {
	case <-bp.stopC:
		return true
	default:
		return false
	}
}

func (bp *basePoller) shouldDrainOnShutdown() bool {
	return bp.workerPollCompleteOnShutdown != nil && bp.workerPollCompleteOnShutdown.Load()
}

// doPoll runs the given pollFunc in a separate go routine. Returns when any of the conditions are met:
//   - poll succeeds
//   - poll fails
//   - worker is stopping
//
// doPoll 执行一次带 context 的 gRPC 长轮询调用。
//
// pollFunc 是具体的 Poll 函数（如 PollWorkflowTaskQueue / PollActivityTaskQueue），
// 在独立 goroutine 中异步执行。当前 goroutine 通过 select 监听两种结果：
//   - doneC: Poll 完成（成功或失败）
//   - stopC: Worker 正在停止
//
// Shutdown 行为分两种模式（由 Server Capabilities 的 WorkerPollCompleteOnShutdown 决定）：
//
//   【新模式】workerPollCompleteOnShutdown == true
//     Server 收到 ShutdownWorker RPC 后会返回空响应来结束 Poll。
//     因此收到 stopC 后不立即 cancel，而是等 5 秒让 Server 自然完成 Poll。
//     5 秒超时后兜底 cancel（防止 gRPC 连接已断开导致永不等完成）。
//
//   【旧模式】workerPollCompleteOnShutdown == false (legacy)
//     Server 不支持此特性，收到 stopC 后立即 cancel gRPC context，
//     直接返回 errStop。
func (bp *basePoller) doPoll(pollFunc func(ctx context.Context) (taskForWorker, error)) (taskForWorker, error) {
	// 快速路径：如果 Worker 已经在停了，不要发起新的 Poll
	if bp.stopping() {
		return nil, errStop
	}

	var err error
	var result taskForWorker

	doneC := make(chan struct{})
	// 构造 gRPC context：带长轮询超时（如 60s）+ 长轮询标记
	ctx, cancel := newGRPCContext(context.Background(), grpcTimeout(pollTaskServiceTimeOut), grpcLongPoll(true))

	// Poll 在独立 goroutine 中异步执行——主 goroutine 用 select 同时监听 doneC 和 stopC
	go func() {
		result, err = pollFunc(ctx)
		cancel()
		close(doneC)
	}()

	if bp.shouldDrainOnShutdown() {
		// Don't cancel the gRPC stream. After ShutdownWorker, the server
		// completes the poll with an empty response. The poll is bounded
		// by the gRPC timeout (pollTaskServiceTimeOut). Stop() waits for
		// all pollers to finish before proceeding to task drain.
		<-doneC
		return result, err
	}

	// ===== 旧模式：立即 cancel（legacy） =====
	select {
	case <-doneC:
		return result, err
	case <-bp.stopC:
		// Server 不支持自然完成 Poll → 直接 cancel gRPC context → 立刻返回
		cancel()
		return nil, errStop
	}
}

func (bp *basePoller) getCapabilities() *workflowservice.GetSystemInfoResponse_Capabilities {
	if bp.capabilities == nil {
		return &workflowservice.GetSystemInfoResponse_Capabilities{}
	}
	return bp.capabilities
}

func (bp *basePoller) getDeploymentName() string {
	return bp.workerDeploymentVersion.DeploymentName
}

// newWorkflowTaskProcessor creates a new workflow task poller which must have a one to one relationship to workflow worker
func newWorkflowTaskProcessor(
	taskHandler WorkflowTaskHandler,
	contextManager WorkflowContextManager,
	service workflowservice.WorkflowServiceClient,
	params workerExecutionParameters,
	stickyUUID string,
) *workflowTaskProcessor {
	return &workflowTaskProcessor{
		basePoller: basePoller{
			metricsHandler:               params.MetricsHandler,
			stopC:                        params.WorkerStopChannel,
			workerBuildID:                params.getBuildID(),
			useBuildIDVersioning:         params.UseBuildIDForVersioning,
			workerDeploymentVersion:      params.DeploymentOptions.Version,
			capabilities:                 params.capabilities,
			pollTimeTracker:              params.pollTimeTracker,
			workerInstanceKey:            params.workerInstanceKey,
			workerPollCompleteOnShutdown: params.workerPollCompleteOnShutdown,
		},
		service:                      service,
		namespace:                    params.Namespace,
		taskQueueName:                params.TaskQueue,
		identity:                     params.Identity,
		taskHandler:                  taskHandler,
		contextManager:               contextManager,
		logger:                       params.Logger,
		dataConverter:                params.DataConverter,
		failureConverter:             params.FailureConverter,
		stickyUUID:                   stickyUUID,
		StickyScheduleToStartTimeout: params.StickyScheduleToStartTimeout,
		stickyCacheSize:              params.cache.MaxWorkflowCacheSize(),
		eagerActivityExecutor:        params.eagerActivityExecutor,
		numNormalPollerMetric:        newNumPollerMetric(params.MetricsHandler, metrics.PollerTypeWorkflowTask),
		numStickyPollerMetric:        newNumPollerMetric(params.MetricsHandler, metrics.PollerTypeWorkflowStickyTask),
		inboundPayloadVisitor:        params.inboundPayloadVisitor,
		outboundPayloadVisitor:       params.outboundPayloadVisitor,
		payloadVisitorConcurrency:    params.payloadVisitorConcurrency,
	}
}

// PollTask polls a new task
func (wtp *workflowTaskPoller) PollTask() (taskForWorker, error) {
	// Get the task.
	workflowTask, err := wtp.doPoll(wtp.poll)
	if err != nil {
		return nil, err
	}

	return workflowTask, nil
}

func (wtp *workflowTaskProcessor) createPoller(mode workflowTaskPollerMode) taskPoller {
	return &workflowTaskPoller{
		basePoller:                   wtp.basePoller,
		mode:                         mode,
		namespace:                    wtp.namespace,
		taskQueueName:                wtp.taskQueueName,
		identity:                     wtp.identity,
		service:                      wtp.service,
		taskHandler:                  wtp.taskHandler,
		contextManager:               wtp.contextManager,
		logger:                       wtp.logger,
		dataConverter:                wtp.dataConverter,
		failureConverter:             wtp.failureConverter,
		stickyUUID:                   wtp.stickyUUID,
		StickyScheduleToStartTimeout: wtp.StickyScheduleToStartTimeout,
		pendingRegularPollCount:      wtp.pendingRegularPollCount,
		pendingStickyPollCount:       wtp.pendingStickyPollCount,
		stickyBacklog:                wtp.stickyBacklog,
		stickyCacheSize:              wtp.stickyCacheSize,
		eagerActivityExecutor:        wtp.eagerActivityExecutor,
		numNormalPollerMetric:        wtp.numNormalPollerMetric,
		numStickyPollerMetric:        wtp.numStickyPollerMetric,
		inboundPayloadVisitor:        wtp.inboundPayloadVisitor,
		payloadVisitorConcurrency:    wtp.payloadVisitorConcurrency,
	}
}

// ProcessTask processes a task which could be workflow task or local activity result
func (wtp *workflowTaskProcessor) ProcessTask(task interface{}) error {
	if !wtp.shouldDrainOnShutdown() && wtp.stopping() {
		return errStop
	}

	switch task := task.(type) {
	case *workflowTask:
		return wtp.processWorkflowTask(task)
	case *eagerWorkflowTask:
		return wtp.processWorkflowTask(wtp.toWorkflowTask(task.task))
	default:
		panic("unknown task type.")
	}
}

func (wtp *workflowTaskProcessor) processWorkflowTask(task *workflowTask) (retErr error) {
	// task.task == nil 表示这次 poll 没拿到真正的 Workflow Task。
	// 这种情况通常是长轮询超时/空响应；base worker 仍会把它当作一次空 task 交到这里。
	if task.task == nil {
		// We didn't have task, poll might have timeout.
		traceLog(func() {
			wtp.logger.Debug("Workflow task unavailable")
		})
		// 空 task 不需要回复 server，也不需要触发 workflow 执行。
		return nil
	}

	// doneCh 用来通知 local activity worker：当前 workflow task 处理流程已经结束。
	// 如果 workflow task 提前结束，local activity 的结果发送方不能永远阻塞在 laResultCh 上。
	doneCh := make(chan struct{})
	// laResultCh 用来接收 local activity 执行完成后的结果。
	// workflow task handler 可能在处理 workflow task 时等待 local activity 结果。
	laResultCh := make(chan *localActivityResult)
	// laRetryCh 用来接收需要 retry 的 local activity task。
	laRetryCh := make(chan *localActivityTask)
	// close doneCh so local activity worker won't get blocked forever when trying to send back result to laResultCh.
	// 无论函数怎么返回，都关闭 doneCh，释放 local activity 相关 goroutine。
	defer close(doneCh)

	// downloadPayloadMetrics 统计这次处理 workflow task 时，从外部 payload storage 下载 payload 的数量/大小/耗时。
	downloadPayloadMetrics := &workflowTaskStorageMetrics{}
	// 把 storage callback 放进 context，后面 inbound payload visitor 访问外部 payload 时会更新指标。
	ctx := extstore.WithStorageOperationCallback(context.Background(), downloadPayloadMetrics)

	// taskErr 表示 workflow task 处理过程中产生的错误。
	// defer 里会根据它决定 workflow context 是否可继续缓存。
	var taskErr error
	// 在正式处理 task 之前，先遍历 poll response 里的 payload。
	// inboundPayloadVisitor 可能会解密、解压、从外部 storage 拉 payload，或做 payload 校验。
	if taskErr = visitProtoPayloads(ctx, wtp.inboundPayloadVisitor, task.task, wtp.payloadVisitorConcurrency); taskErr != nil {
		// inbound payload 处理失败时，SDK 会尝试向 server 上报 query/workflow task 失败。
		wtp.handleInboundVisitorError(task.task, taskErr)
		// 错误已经通过 handleInboundVisitorError 处理，这里不再把错误返回给 baseWorker。
		return nil
	}

	// 获取或创建这个 workflow execution 对应的本地 workflow context。
	// 如果 sticky cache 命中，会复用已有 context；否则会创建新 context 并通过 history replay 恢复状态。
	wfctx, err := wtp.contextManager.GetOrCreateWorkflowContext(task.task, task.historyIterator)
	if err != nil {
		// context 获取失败通常表示 history/replay/cache 状态有问题，交给上层记录处理。
		return err
	}
	// 函数结束时必须 unlock workflow context。
	// 如果处理过程中 panic 或出错，要把错误传给 Unlock，让缓存逻辑知道这个 context 不能安全复用。
	defer func() {
		// If we panic during processing the workflow task, we need to unlock the workflow context with an error to discard it.
		if p := recover(); p != nil {
			// workflow task 处理 panic 时，生成带 task queue 的 stack trace 标题。
			topLine := fmt.Sprintf("workflow task for %s [panic]:", wtp.taskQueueName)
			st := getStackTraceRaw(topLine, 7, 0)
			// 记录 workflow id/run id/workflow type/attempt/panic/stack，方便定位是哪个 workflow task 崩了。
			wtp.logger.Error("Workflow task processing panic.",
				tagWorkflowID, task.task.WorkflowExecution.GetWorkflowId(),
				tagRunID, task.task.WorkflowExecution.GetRunId(),
				tagWorkerType, task.task.GetWorkflowType().Name,
				tagAttempt, task.task.Attempt,
				tagPanicError, fmt.Sprintf("%v", p),
				tagPanicStack, st)
			// 把 panic 转成 error，让后续 Unlock 丢弃该 workflow context。
			taskErr = newPanicError(p, st)
			// retErr 返回给 baseWorker，baseWorker 会记录 task processing failed。
			retErr = taskErr
		}
		// 解锁 workflow context。
		// taskErr == nil 时，context 可以继续留在 sticky cache；
		// taskErr != nil 时，context 通常会被视为不安全并丢弃/重置。
		wfctx.Unlock(taskErr)
	}()

	// 这个 for 循环用于处理“同一次 RespondWorkflowTaskCompleted 后 server 立即返回新 WorkflowTask”的情况。
	// 例如 server 在 RespondWorkflowTaskCompletedResponse.WorkflowTask 中直接带回下一个 workflow task，
	// SDK 可以不重新走 poll，继续在当前 goroutine 里处理新 task。
	for {
		// 记录本轮 workflow task 处理开始时间，用于执行耗时指标和慢 task 日志。
		startTime := time.Now()
		// 把 local activity 生命周期相关 channel 挂到当前 workflowTask 上。
		// taskHandler.ProcessWorkflowTask 内部会用这些 channel 协调 local activity 结果/重试。
		task.doneCh = doneCh
		task.laResultCh = laResultCh
		task.laRetryCh = laRetryCh
		// taskCompletion 是 taskHandler 处理后生成的响应包装：
		// 可能是 RespondWorkflowTaskCompleted、RespondWorkflowTaskFailed 或 RespondQueryTaskCompleted。
		var taskCompletion *workflowTaskCompletion
		// 调用 WorkflowTaskHandler 处理当前 workflow task。
		// 这里会 replay history、执行 workflow 代码/query handler、生成 commands 或 failure response。
		taskCompletion, taskErr = wtp.taskHandler.ProcessWorkflowTask(
			task,
			wfctx,
			// 这个 callback 是 workflow task heartbeat/force complete 路径使用的。
			// 当 handler 需要中途强制 RespondWorkflowTaskCompleted 时，通过它把当前 completion 先发给 server。
			func(taskCompletion *workflowTaskCompletion, startTime time.Time) (*workflowTask, error) {
				wtp.logger.Debug("Force RespondWorkflowTaskCompleted.", "TaskStartedEventID", task.task.GetStartedEventId())
				// 把当前 completion 发回 server，并记录 metrics。
				heartbeatResponse, err := wtp.RespondTaskCompletedWithMetrics(
					taskCompletion,
					nil,
					task.task,
					startTime,
					downloadPayloadMetrics,
					wfctx.workflowInfo)
				if err != nil {
					return nil, err
				}
				// server 没有在 heartbeat response 里带新 workflow task，说明当前没有可继续处理的 task。
				if heartbeatResponse == nil || heartbeatResponse.WorkflowTask == nil {
					return nil, nil
				}
				// server 返回了新 workflow task，把公开 API response 包装成 SDK 内部 workflowTask。
				task := wtp.toWorkflowTask(heartbeatResponse.WorkflowTask)
				// 新 task 也要先处理 inbound payload。
				if err := visitProtoPayloads(ctx, wtp.inboundPayloadVisitor, task.task, wtp.payloadVisitorConcurrency); err != nil {
					wtp.handleInboundVisitorError(task.task, err)
					return nil, nil
				}
				// 复用同一组 local activity channel。
				task.doneCh = doneCh
				task.laResultCh = laResultCh
				task.laRetryCh = laRetryCh
				// 返回新 task 给 handler，让它继续处理。
				return task, nil
			},
		)
		// taskCompletion 和 taskErr 都为空，表示 handler 已经处理完但不需要回复 server。
		// 例如某些空/无需处理路径。
		if taskCompletion == nil && taskErr == nil {
			return nil
		}
		// workflowTaskHeartbeatError 表示 heartbeat/force complete 路径已经决定返回错误。
		// 这里直接返回，避免再走一次普通 RespondTaskCompletedWithMetrics。
		if _, ok := taskErr.(workflowTaskHeartbeatError); ok {
			return taskErr
		}
		// 正常完成或失败都通过这里统一回复 server：
		// - taskErr == nil：发送 taskCompletion 中的 completed/query completed 请求；
		// - taskErr != nil：转换为 RespondWorkflowTaskFailed。
		response, err := wtp.RespondTaskCompletedWithMetrics(
			taskCompletion,
			taskErr,
			task.task,
			startTime,
			downloadPayloadMetrics,
			wfctx.workflowInfo)
		if err != nil {
			// If we get an error responding to the workflow task we need to evict the execution from the cache.
			// 回复 server 失败时，不能继续信任本地 workflow context。
			// 把 taskErr 设成 err，defer Unlock 会据此丢弃/重置 context。
			taskErr = err
			return err
		}

		// server 可能要求 SDK 把本地 workflow context 的 previous started event id 回退到某个 event。
		// 这用于 history reset/修正 sticky replay 边界。
		if eventLevel := response.GetResetHistoryEventId(); eventLevel != 0 {
			wfctx.SetPreviousStartedEventID(eventLevel)
		}

		// 没有 response、response 没带新 workflow task，或者本轮 taskErr 非空，
		// 都说明当前处理链路结束，不再继续循环。
		if response == nil || response.WorkflowTask == nil || taskErr != nil {
			return nil
		}

		// we are getting new workflow task, so reset the workflowTask and continue process the new one
		// server 在 RespondWorkflowTaskCompletedResponse 里直接带回了新 workflow task。
		// 这是一种避免重新 poll 的优化，继续循环处理它。
		task = wtp.toWorkflowTask(response.WorkflowTask)
		// 新 task 进入处理前同样要跑 inbound payload visitor。
		if err := visitProtoPayloads(ctx, wtp.inboundPayloadVisitor, task.task, wtp.payloadVisitorConcurrency); err != nil {
			wtp.handleInboundVisitorError(task.task, err)
			return nil
		}
	}
}

func (wtp *workflowTaskProcessor) RespondTaskCompletedWithMetrics(
	taskCompletion *workflowTaskCompletion,
	taskErr error,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	startTime time.Time,
	downloadPayloadMetrics *workflowTaskStorageMetrics,
	workflowInfo *WorkflowInfo,
) (response *workflowservice.RespondWorkflowTaskCompletedResponse, err error) {
	metricsHandler := wtp.metricsHandler.WithTags(metrics.WorkflowTags(task.WorkflowType.GetName()))

	emitFailMetric := false
	var failureReason string
	if taskErr != nil {
		wtp.logger.Warn("Failed to process workflow task.",
			tagWorkflowType, task.WorkflowType.GetName(),
			tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
			tagRunID, task.WorkflowExecution.GetRunId(),
			tagAttempt, task.Attempt,
			tagError, taskErr)
		emitFailMetric = true
		failWorkflowTask := wtp.errorToFailWorkflowTask(task.TaskToken, taskErr)
		failureReason = "WorkflowError"
		if failWorkflowTask.Cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR {
			failureReason = "NonDeterminismError"
		}
		taskCompletion = &workflowTaskCompletion{rawRequest: failWorkflowTask}
	}

	uploadPayloadMetrics := &workflowTaskStorageMetrics{}
	ctx := extstore.WithStorageOperationCallback(context.Background(), uploadPayloadMetrics)
	ctx = extstore.WithStorageTarget(ctx, extstore.StorageDriverWorkflowInfo{
		Namespace:    wtp.namespace,
		WorkflowID:   task.WorkflowExecution.GetWorkflowId(),
		RunID:        task.WorkflowExecution.GetRunId(),
		WorkflowType: task.WorkflowType.GetName(),
	})
	outboundPayloadVisitor := &commandAwarePayloadVisitor{
		innerVisitor: wtp.outboundPayloadVisitor,
		workflowInfo: workflowInfo,
	}
	if taskErr = visitProtoPayloads(ctx, outboundPayloadVisitor, taskCompletion.rawRequest, wtp.payloadVisitorConcurrency); taskErr != nil {
		// The outbound visitor failed (e.g. storage driver error or panic). We
		// cannot send the original response, so fall back to an explicit WFT
		// failure so the server records the error immediately.
		keyvals := []any{
			tagWorkflowType, task.WorkflowType.GetName(),
			tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
			tagRunID, task.WorkflowExecution.GetRunId(),
			tagAttempt, task.Attempt,
		}
		var errPayloadSize payloadSizeError
		if errors.As(taskErr, &errPayloadSize) {
			keyvals = append(keyvals,
				tagPayloadSize, errPayloadSize.size,
				tagPayloadSizeLimit, errPayloadSize.limit)
		}
		wtp.logger.Warn("Workflow task postprocess error: "+taskErr.Error(), keyvals...)
		emitFailMetric = true
		failureReason = "WorkflowError"
		if errors.As(taskErr, new(payloadSizeError)) {
			failureReason = "PayloadsTooLarge"
		}
		taskCompletion = &workflowTaskCompletion{rawRequest: wtp.errorToFailWorkflowTask(task.TaskToken, taskErr)}
	}

	taskDuration := time.Since(startTime)
	metricsHandler.Timer(metrics.WorkflowTaskExecutionLatency).Record(taskDuration)

	response, err = wtp.sendTaskCompletedRequest(taskCompletion, task)

	completionEventId := task.GetStartedEventId() + 1
	loggerDurationKeyVals := []interface{}{
		tagWorkflowType, task.WorkflowType.GetName(),
		tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
		tagRunID, task.WorkflowExecution.GetRunId(),
		tagAttempt, task.Attempt,
		tagEventID, completionEventId,
		tagWorkflowTaskDuration, taskDuration,
	}
	if downloadPayloadMetrics.payloadCount > 0 {
		loggerDurationKeyVals = append(loggerDurationKeyVals,
			tagPayloadDownloadCount, downloadPayloadMetrics.payloadCount,
			tagPayloadDownloadSize, downloadPayloadMetrics.totalSize,
			tagPayloadDownloadDuration, downloadPayloadMetrics.totalDuration,
			tagPayloadDownloadDrivers, downloadPayloadMetrics.GetDriverNames(),
		)
	}
	if uploadPayloadMetrics.payloadCount > 0 {
		loggerDurationKeyVals = append(loggerDurationKeyVals,
			tagPayloadUploadCount, uploadPayloadMetrics.payloadCount,
			tagPayloadUploadSize, uploadPayloadMetrics.totalSize,
			tagPayloadUploadDuration, uploadPayloadMetrics.totalDuration,
			tagPayloadUploadDrivers, uploadPayloadMetrics.GetDriverNames(),
		)
	}

	taskID := fmt.Sprintf("%s:%d:%d", task.WorkflowExecution.GetRunId(), completionEventId, task.Attempt)
	if taskDuration > 10*time.Second {
		wtp.logger.Warn("[TMPRL1104] "+taskID+" Workflow task exceeded 10 seconds.", loggerDurationKeyVals...)
	} else if taskDuration > 5*time.Second {
		wtp.logger.Info("[TMPRL1104] "+taskID+" Workflow task exceeded 5 seconds.", loggerDurationKeyVals...)
	} else {
		traceLog(func() {
			wtp.logger.Debug("Workflow task duration information.", loggerDurationKeyVals...)
		})
	}

	var grpcMessageTooLargeErr *retry.GrpcMessageTooLargeError
	if errors.As(err, &grpcMessageTooLargeErr) {
		secondEmitFailMetric, secondErr := wtp.reportGrpcMessageTooLarge(ctx, taskCompletion, task, err)
		if secondEmitFailMetric {
			emitFailMetric = true
			// Overwriting the original failure reason for metrics purposes
			failureReason = "GrpcMessageTooLarge"
		}
		// We already know the first error was GRPC message too large, if there was another error when reporting the first error
		// to the server it's probably more interesting for the user.
		if secondErr != nil {
			err = secondErr
		}
	}

	if emitFailMetric {
		incrementWorkflowTaskFailureCounter(metricsHandler, failureReason)
	}

	return
}

func (wtp *workflowTaskProcessor) sendTaskCompletedRequest(
	taskCompletion *workflowTaskCompletion,
	task *workflowservice.PollWorkflowTaskQueueResponse,
) (response *workflowservice.RespondWorkflowTaskCompletedResponse, err error) {
	ctx := context.Background()
	// Respond task completion.
	grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(
		wtp.metricsHandler.WithTags(metrics.RPCTags(task.GetWorkflowType().GetName(),
			metrics.NoneTagValue, metrics.NoneTagValue))),
		defaultGrpcRetryParameters(ctx))
	defer cancel()
	if taskCompletion == nil {
		// should not happen
		panic("unknown request type from ProcessWorkflowTask()")
	}
	switch request := taskCompletion.rawRequest.(type) {
	case *workflowservice.RespondWorkflowTaskFailedRequest:
		// Only fail workflow task on first attempt, subsequent failure on the same workflow task will timeout.
		// This is to avoid spin on the failed workflow task. Checking Attempt not nil for older server.
		if task.GetAttempt() == 1 {
			_, err = wtp.service.RespondWorkflowTaskFailed(grpcCtx, request)
			if err != nil {
				traceLog(func() {
					wtp.logger.Debug("RespondWorkflowTaskFailed failed.", tagError, err)
				})
			} else if taskCompletion.applyCompletionMetrics != nil {
				taskCompletion.applyCompletionMetrics()
			}
		}
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		if request.StickyAttributes == nil && wtp.stickyCacheSize > 0 {
			request.StickyAttributes = &taskqueuepb.StickyExecutionAttributes{
				WorkerTaskQueue: &taskqueuepb.TaskQueue{
					Name:       getWorkerTaskQueue(wtp.stickyUUID),
					Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
					NormalName: wtp.taskQueueName,
				},
				ScheduleToStartTimeout: durationpb.New(wtp.StickyScheduleToStartTimeout),
			}
		}
		eagerReserved := wtp.eagerActivityExecutor.applyToRequest(request)
		response, err = wtp.service.RespondWorkflowTaskCompleted(grpcCtx, request)
		if err != nil {
			traceLog(func() {
				wtp.logger.Debug("RespondWorkflowTaskCompleted failed.", tagError, err)
			})
		} else if taskCompletion.applyCompletionMetrics != nil {
			taskCompletion.applyCompletionMetrics()
		}
		wtp.eagerActivityExecutor.handleResponse(response, eagerReserved)
	case *workflowservice.RespondQueryTaskCompletedRequest:
		_, err = wtp.service.RespondQueryTaskCompleted(grpcCtx, request)
		if err != nil {
			traceLog(func() {
				wtp.logger.Debug("RespondQueryTaskCompleted failed.", tagError, err)
			})
		} else if taskCompletion.applyCompletionMetrics != nil {
			taskCompletion.applyCompletionMetrics()
		}
	default:
		// should not happen
		panic("unknown request type from ProcessWorkflowTask()")
	}
	return
}

func (wtp *workflowTaskProcessor) reportGrpcMessageTooLarge(
	ctx context.Context,
	taskCompletion *workflowTaskCompletion,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	sendErr error,
) (emitFailMetric bool, err error) {
	if taskCompletion == nil {
		// should not happen
		panic("unknown request type from ProcessWorkflowTask()")
	}
	switch taskCompletion.rawRequest.(type) {
	case *workflowservice.RespondWorkflowTaskCompletedRequest, *workflowservice.RespondWorkflowTaskFailedRequest:
		emitFailMetric = true
		request := wtp.errorToFailWorkflowTask(task.TaskToken, sendErr)
		request.Cause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE
		if err = visitProtoPayloads(ctx, wtp.outboundPayloadVisitor, request, wtp.payloadVisitorConcurrency); err != nil {
			wtp.logger.Error("Failed to visit payloads for GRPC message too large failure response.", tagError, err)
			return
		}
		_, err = wtp.sendTaskCompletedRequest(&workflowTaskCompletion{rawRequest: request}, task)
	case *workflowservice.RespondQueryTaskCompletedRequest:
		request := &workflowservice.RespondQueryTaskCompletedRequest{
			TaskToken:     task.TaskToken,
			CompletedType: enumspb.QUERY_RESULT_TYPE_FAILED,
			ErrorMessage:  sendErr.Error(),
			Namespace:     wtp.namespace,
			Failure:       wtp.failureConverter.ErrorToFailure(sendErr),
			Cause:         enumspb.WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE,
		}
		if err = visitProtoPayloads(ctx, wtp.outboundPayloadVisitor, request, wtp.payloadVisitorConcurrency); err != nil {
			wtp.logger.Error("Failed to visit payloads for GRPC message too large query failure response.", tagError, err)
			return
		}
		_, err = wtp.sendTaskCompletedRequest(&workflowTaskCompletion{rawRequest: request}, task)
	default:
		// should not happen
		panic("unknown request type from ProcessWorkflowTask()")
	}
	return
}

func (wtp *workflowTaskProcessor) handleInboundVisitorError(task *workflowservice.PollWorkflowTaskQueueResponse, visitErr error) {
	keyvals := []any{
		tagWorkflowType, task.WorkflowType.GetName(),
		tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
		tagRunID, task.WorkflowExecution.GetRunId(),
		tagAttempt, task.Attempt,
	}
	var errPayloadSize payloadSizeError
	if errors.As(visitErr, &errPayloadSize) {
		keyvals = append(keyvals,
			tagPayloadSize, errPayloadSize.size,
			tagPayloadSizeLimit, errPayloadSize.limit)
	}
	wtp.logger.Warn("Workflow task preprocess error: "+visitErr.Error(), keyvals...)
	// Submit an explicit WFT failure so the server records the error immediately
	// rather than waiting for the task to time out.
	failReq := wtp.errorToFailWorkflowTask(task.TaskToken, visitErr)
	if _, submitErr := wtp.sendTaskCompletedRequest(&workflowTaskCompletion{rawRequest: failReq}, task); submitErr != nil {
		wtp.logger.Warn("Failed to submit WFT failure after inbound visitor error.", tagError, submitErr)
	}
}

func (wtp *workflowTaskProcessor) errorToFailWorkflowTask(taskToken []byte, err error) *workflowservice.RespondWorkflowTaskFailedRequest {
	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	// If it was a panic due to a bad state machine or if it was a history
	// mismatch error, mark as non-deterministic
	if panicErr, _ := err.(*workflowPanicError); panicErr != nil {
		if _, badStateMachine := panicErr.value.(stateMachineIllegalStatePanic); badStateMachine {
			cause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR
		}
	} else if _, mismatch := err.(historyMismatchError); mismatch {
		cause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR
	} else if _, unknown := err.(unknownSdkFlagError); unknown {
		cause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR
	} else if errors.As(err, new(payloadSizeError)) {
		cause = enumspb.WORKFLOW_TASK_FAILED_CAUSE_PAYLOADS_TOO_LARGE
	}

	return wtp.errorToFailWorkflowTaskWithCause(taskToken, err, cause)
}

func (wtp *workflowTaskProcessor) errorToFailWorkflowTaskWithCause(taskToken []byte, err error, cause enumspb.WorkflowTaskFailedCause) *workflowservice.RespondWorkflowTaskFailedRequest {
	builtRequest := &workflowservice.RespondWorkflowTaskFailedRequest{
		TaskToken:      taskToken,
		Cause:          cause,
		Failure:        wtp.failureConverter.ErrorToFailure(err),
		Identity:       wtp.identity,
		BinaryChecksum: wtp.workerBuildID,
		Namespace:      wtp.namespace,
		WorkerVersion: &commonpb.WorkerVersionStamp{
			BuildId:       wtp.workerBuildID,
			UseVersioning: wtp.useBuildIDVersioning,
		},
		Deployment: &deploymentpb.Deployment{
			BuildId:    wtp.workerBuildID,
			SeriesName: wtp.getDeploymentName(),
		},
		DeploymentOptions: workerDeploymentOptionsToProto(
			wtp.useBuildIDVersioning,
			wtp.workerDeploymentVersion,
		),
	}

	if wtp.getCapabilities().BuildIdBasedVersioning {
		//lint:ignore SA1019 ignore deprecated versioning APIs
		builtRequest.BinaryChecksum = ""
	}

	return builtRequest
}

type workflowTaskStorageMetrics struct {
	mu            sync.Mutex
	payloadCount  int
	totalSize     int64
	totalDuration time.Duration
	driverNames   map[string]struct{}
}

func (callback *workflowTaskStorageMetrics) PayloadBatchCompleted(count int, size int64, duration time.Duration, driverNames []string) {
	callback.mu.Lock()
	defer callback.mu.Unlock()
	callback.payloadCount += count
	callback.totalSize += size
	callback.totalDuration += duration
	for _, name := range driverNames {
		if callback.driverNames == nil {
			callback.driverNames = make(map[string]struct{})
		}
		callback.driverNames[name] = struct{}{}
	}
}

func (callback *workflowTaskStorageMetrics) GetDriverNames() []string {
	names := make([]string, 0, len(callback.driverNames))
	for name := range callback.driverNames {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func newLocalActivityPoller(
	params workerExecutionParameters,
	laTunnel *localActivityTunnel,
	interceptors []WorkerInterceptor,
	client *WorkflowClient,
	workerStopCh <-chan struct{},
) *localActivityTaskPoller {
	handler := &localActivityTaskHandler{
		backgroundContext:  params.BackgroundContext,
		metricsHandler:     params.MetricsHandler,
		logger:             params.Logger,
		dataConverter:      params.DataConverter,
		contextPropagators: params.ContextPropagators,
		interceptors:       interceptors,
		client:             client,
		workerStopChannel:  workerStopCh,
	}
	return &localActivityTaskPoller{
		basePoller:   basePoller{metricsHandler: params.MetricsHandler, stopC: params.WorkerStopChannel},
		handler:      handler,
		logger:       params.Logger,
		laTunnel:     laTunnel,
		workerStopCh: workerStopCh,
	}
}

func (latp *localActivityTaskPoller) PollTask() (taskForWorker, error) {
	return latp.laTunnel.getTask(), nil
}

func (latp *localActivityTaskPoller) ProcessTask(task interface{}) error {
	if latp.stopping() {
		return errStop
	}

	result := latp.handler.executeLocalActivityTask(task.(*localActivityTask))

	// If shutdown is initiated after we begin local activity execution, there is no need to send result back to
	// laResultCh, as both workers receive shutdown from top down.
	if latp.stopping() {
		return errStop
	}
	// We need to send back the local activity result to unblock workflowTaskPoller.processWorkflowTask() which is
	// synchronously listening on the laResultCh. We also want to make sure we don't block here forever in case
	// processWorkflowTask() already returns and nobody is receiving from laResultCh. We guarantee that doneCh is closed
	// before returning from workflowTaskPoller.processWorkflowTask().
	select {
	case result.task.workflowTask.laResultCh <- result:
		return nil
	case <-result.task.workflowTask.doneCh:
		// processWorkflowTask() already returns, just drop this local activity result.
		return nil
	}
}

func (lath *localActivityTaskHandler) executeLocalActivityTask(task *localActivityTask) (result *localActivityResult) {
	workflowType := task.params.WorkflowInfo.WorkflowType.Name
	activityType := task.params.ActivityType
	metricsHandler := lath.metricsHandler.WithTags(metrics.LocalActivityTags(workflowType, activityType))

	metricsHandler.Counter(metrics.LocalActivityTotalCounter).Inc(1)

	ae := activityExecutor{name: activityType, fn: task.params.ActivityFn}
	traceLog(func() {
		lath.logger.Debug("Processing new local activity task",
			tagWorkflowID, task.params.WorkflowInfo.WorkflowExecution.ID,
			tagRunID, task.params.WorkflowInfo.WorkflowExecution.RunID,
			tagActivityType, activityType,
			tagAttempt, task.attempt,
		)
	})
	ctx, err := WithLocalActivityTask(lath.backgroundContext, task, lath.logger, lath.metricsHandler,
		lath.dataConverter, lath.interceptors, lath.client, lath.workerStopChannel)
	if err != nil {
		return &localActivityResult{task: task, err: fmt.Errorf("failed building context: %w", err)}
	}

	// propagate context information into the local activity context from the headers
	ctx, err = contextWithHeaderPropagated(ctx, task.header, lath.contextPropagators)
	if err != nil {
		return &localActivityResult{task: task, err: err}
	}

	info := getActivityEnv(ctx)
	ctx, cancel := context.WithDeadline(ctx, info.deadline)
	defer cancel()

	task.Lock()
	if task.canceled {
		task.Unlock()
		return &localActivityResult{err: ErrCanceled, task: task}
	}
	task.attemptsThisWFT += 1
	task.cancelFunc = cancel
	task.Unlock()

	var laResult *commonpb.Payloads
	doneCh := make(chan struct{})
	go func(ch chan struct{}) {
		laStartTime := time.Now()
		defer close(ch)

		// panic handler
		defer func() {
			if p := recover(); p != nil {
				topLine := fmt.Sprintf("local activity for %s [panic]:", activityType)
				st := getStackTraceRaw(topLine, 7, 0)
				lath.logger.Error("LocalActivity panic.",
					tagWorkflowID, task.params.WorkflowInfo.WorkflowExecution.ID,
					tagRunID, task.params.WorkflowInfo.WorkflowExecution.RunID,
					tagActivityType, activityType,
					tagAttempt, task.attempt,
					tagPanicError, fmt.Sprintf("%v", p),
					tagPanicStack, st)
				metricsHandler.Counter(metrics.LocalActivityErrorCounter).Inc(1)
				err = newPanicError(p, st)
			}
			if err != nil && !isBenignApplicationError(err) {
				metricsHandler.Counter(metrics.LocalActivityFailedCounter).Inc(1)
				metricsHandler.Counter(metrics.LocalActivityExecutionFailedCounter).Inc(1)
			}
		}()

		laResult, err = ae.ExecuteWithActualArgs(ctx, task.params.InputArgs)
		executionLatency := time.Since(laStartTime)
		metricsHandler.Timer(metrics.LocalActivityExecutionLatency).Record(executionLatency)
		if time.Now().After(info.deadline) {
			// If local activity takes longer than expected timeout, the context would already be DeadlineExceeded and
			// the result would be discarded. Print a warning in this case.
			lath.logger.Warn("LocalActivity completed after activity deadline.",
				"LocalActivityID", task.activityID,
				"ActivityDeadline", info.deadline,
				"LocalActivityType", activityType,
				"ScheduleToCloseTimeout", task.params.ScheduleToCloseTimeout,
				"StartToCloseTimeout", task.params.StartToCloseTimeout,
				"ActualExecutionDuration", executionLatency)
		}
	}(doneCh)

WaitResult:
	select {
	case <-ctx.Done():
		select {
		case <-doneCh:
			// double check if result is ready.
			break WaitResult
		default:
		}

		// context is done
		if ctx.Err() == context.Canceled {
			metricsHandler.Counter(metrics.LocalActivityCanceledCounter).Inc(1)
			metricsHandler.Counter(metrics.LocalActivityExecutionCanceledCounter).Inc(1)
			return &localActivityResult{err: ErrCanceled, task: task}
		} else if ctx.Err() == context.DeadlineExceeded {
			if task.params.ScheduleToCloseTimeout != 0 && time.Now().After(info.scheduledTime.Add(task.params.ScheduleToCloseTimeout)) {
				return &localActivityResult{err: ErrDeadlineExceeded, task: task}
			} else {
				return &localActivityResult{err: NewTimeoutError("deadline exceeded", enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil), task: task}
			}
		} else {
			// should not happen
			return &localActivityResult{err: NewApplicationError("unexpected context done", "", true, nil), task: task}
		}
	case <-doneCh:
		// local activity completed
	}

	if err == nil {
		metricsHandler.
			Timer(metrics.LocalActivitySucceedEndToEndLatency).
			Record(time.Since(task.params.ScheduledTime))
	}
	return &localActivityResult{result: laResult, err: err, task: task}
}

func (wtp *workflowTaskPoller) release(kind enumspb.TaskQueueKind) {
	if wtp.stickyCacheSize <= 0 {
		return
	}

	wtp.requestLock.Lock()
	if kind == enumspb.TASK_QUEUE_KIND_STICKY {
		wtp.pendingStickyPollCount--
	} else {
		wtp.pendingRegularPollCount--
	}
	wtp.requestLock.Unlock()
}

func (wtp *workflowTaskPoller) updateBacklog(taskQueueKind enumspb.TaskQueueKind, backlogCountHint int64) {
	if taskQueueKind == enumspb.TASK_QUEUE_KIND_NORMAL || wtp.stickyCacheSize <= 0 {
		// we only care about sticky backlog for now.
		return
	}
	wtp.requestLock.Lock()
	wtp.stickyBacklog = backlogCountHint
	wtp.requestLock.Unlock()
}

// getNextPollRequest returns appropriate next poll request based on poller configuration and mode.
// Simple rules:
//  1. if mode is NonSticky, always poll from regular task queue
//  2. if mode is Sticky, always poll from sticky task queue
//  3. if mode is Mixed
//     3.1. if sticky execution is disabled, always poll for regular task queue
//     3.2. otherwise:
//     3.2.1) if sticky task queue has backlog, always prefer to process sticky task first
//     3.2.2) poll from the task queue that has less pending requests (prefer sticky when they are the same).
// getNextPollRequest 根据 Poller 模式决定本次 Poll 请求的目标队列（Sticky 或 Normal）。
//
// WorkflowTask 有三种 Poller 模式（由 MaxConcurrentWorkflowTaskPollers 等配置决定）：
//
//   NonSticky — 永远只 Poll Normal Queue
//       适用场景：Worker 未启用 Sticky（stickyCacheSize <= 0）
//
//   Sticky — 永远只 Poll Sticky Queue
//       适用场景：Worker 被严格绑定到 Sticky（如只有 1 个 poller）
//       Sticky Queue = Server 会把后续 Task 优先发给上次执行该 Workflow 的 Worker，
//       利用本地缓存跳过 History replay。
//
//   Mixed — 动态在两种队列间切换（默认模式）
//       决策规则（优先级从高到低）：
//       ① 如果 stickyBacklog > 0 → Poll Sticky
//          Server 说 Sticky 队列有积压，优先消化
//       ② 如果 pendingStickyPoll <= pendingRegularPoll → Poll Sticky
//          保持 Sticky 和 Regular 的 inflight 请求数大致 1:1
//       ③ 否则 → Poll Normal
//
//       pendingStickyPollCount / pendingRegularPollCount 分别记录当前有多少个
//       inflight 的 Poll 请求（getNextPollRequest 增，release() 减），用于让
//       Mixed 模式维持两种队列的并发度平衡。
func (wtp *workflowTaskPoller) getNextPollRequest() (request *workflowservice.PollWorkflowTaskQueueRequest) {
	// 默认构造 Normal Queue 请求
	taskQueue := &taskqueuepb.TaskQueue{
		Name: wtp.taskQueueName,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	if wtp.mode == NonSticky || wtp.stickyCacheSize <= 0 {
		// 模式 A: 只用 Normal Queue，taskQueue 已设置完毕
	} else if wtp.mode == Sticky {
		// 模式 B: 只用 Sticky Queue
		// Sticky Queue 名称 = 原始 taskQueueName + stickyUUID 后缀，
		// NormalName 保留原始名称供 Server 做 fallback 匹配
		taskQueue.Name = getWorkerTaskQueue(wtp.stickyUUID)
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
		taskQueue.NormalName = wtp.taskQueueName
	} else if wtp.mode == Mixed {
		// 模式 C: 动态切换（默认）
		wtp.requestLock.Lock()
		// ① 优先消化 stickyBacklog
		// ② 否则维持 Sticky/Normal 的 inflight 数均衡
		if wtp.stickyBacklog > 0 || wtp.pendingStickyPollCount <= wtp.pendingRegularPollCount {
			wtp.pendingStickyPollCount++
			taskQueue.Name = getWorkerTaskQueue(wtp.stickyUUID)
			taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
			taskQueue.NormalName = wtp.taskQueueName
		} else {
			wtp.pendingRegularPollCount++
		}
		wtp.requestLock.Unlock()
	} else {
		panic("unknown workflow task poller mode")
	}

	// 组装完整的 PollWorkflowTaskQueue 请求
	builtRequest := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:      wtp.namespace,
		TaskQueue:      taskQueue,
		Identity:       wtp.identity,
		BinaryChecksum: wtp.workerBuildID,
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId:              wtp.workerBuildID,
			UseVersioning:        wtp.useBuildIDVersioning,
			DeploymentSeriesName: wtp.getDeploymentName(),
		},
		DeploymentOptions: workerDeploymentOptionsToProto(
			wtp.useBuildIDVersioning,
			wtp.workerDeploymentVersion,
		),
		WorkerInstanceKey: wtp.workerInstanceKey,
	}
	if wtp.getCapabilities().BuildIdBasedVersioning {
		builtRequest.BinaryChecksum = ""
	}
	return builtRequest
}

// Poll the workflow task queue and update the num_poller metric
func (wtp *workflowTaskPoller) pollWorkflowTaskQueue(ctx context.Context, request *workflowservice.PollWorkflowTaskQueueRequest) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	if request.TaskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_NORMAL {
		wtp.numNormalPollerMetric.increment()
		defer wtp.numNormalPollerMetric.decrement()
	} else {
		wtp.numStickyPollerMetric.increment()
		defer wtp.numStickyPollerMetric.decrement()
	}

	return wtp.service.PollWorkflowTaskQueue(ctx, request)
}

// Poll for a single workflow task from the service
// poll 是 workflowTaskPoller 的具体 Poll 实现，被 basePoller.doPoll() 作为 pollFunc 参数调用。
//
// 它发一次 PollWorkflowTaskQueue gRPC 请求并处理响应。关键逻辑：
//
//   1. getNextPollRequest() — 决定这次 Poll Sticky Queue 还是 Normal Queue
//      - Sticky (黏性): Server 优先把 Task 发给上次执行该 Workflow 的 Worker
//        → Worker 本地已有缓存，无需重放全量 History，大幅降低延迟
//      - Normal (普通): 首次执行或 Sticky 失效后的 fallback
//      - Mixed 模式下根据 stickyBacklog + pendingPollCount 动态决策比例
//
//   2. pollWorkflowTaskQueue() — 🔴 gRPC 长轮询调用
//
//   3. 空响应处理 — Server 没有可分配的 Task 时返回空 TaskToken
//      → 返回空的 &workflowTask{} (isEmpty()=true)，让 autoscaler 知道这次 Poll 是空的
//
//   4. 成功拿到 Task → 记录指标 → 转换为 workflowTask → 返回
func (wtp *workflowTaskPoller) poll(ctx context.Context) (taskForWorker, error) {
	traceLog(func() {
		wtp.logger.Debug("workflowTaskPoller::Poll")
	})

	// ===== 步骤 1：决定 Poll 哪个队列（Sticky vs Normal）并构造请求 =====
	request := wtp.getNextPollRequest()
	// defer 释放 getNextPollRequest 中递增的 pendingPollCount 计数
	defer wtp.release(request.TaskQueue.GetKind())

	// ===== 步骤 2：🔥 gRPC 长轮询调用 =====
	response, err := wtp.pollWorkflowTaskQueue(ctx, request)
	if err != nil {
		// Poll 失败 → 清空 backlog 估计（避免基于过期数据做决策）
		wtp.updateBacklog(request.TaskQueue.GetKind(), 0)
		return nil, err
	}

	// ===== 步骤 3：空响应 =====
	// Server 没有此 Worker 能处理的 Task → 返回空 TaskToken
	if response == nil || len(response.TaskToken) == 0 {
		wtp.metricsHandler.Counter(metrics.WorkflowTaskQueuePollEmptyCounter).Inc(1)
		wtp.updateBacklog(request.TaskQueue.GetKind(), 0)
		// 返回空的 workflowTask：autoscaler 的 handleTask() 会因其 isEmpty()=true 而触发缩容
		return &workflowTask{}, nil
	}

	// ===== 步骤 4：成功拿到 Task =====
	// 记录 Poll 成功耗时（Sticky 和非 Sticky 分开统计）
	if request.TaskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		wtp.pollTimeTracker.recordPollSuccess(metrics.PollerTypeWorkflowStickyTask)
	} else {
		wtp.pollTimeTracker.recordPollSuccess(metrics.PollerTypeWorkflowTask)
	}

	// 更新 backlog 估计（Server 告知当前队列积压量，用于 Mixed 模式决策 Sticky/Normal 比例）
	wtp.updateBacklog(request.TaskQueue.GetKind(), response.GetBacklogCountHint())

	// 将 protobuf 响应转换为内部 workflowTask 结构体（含 History iterator 等）
	task := wtp.toWorkflowTask(response)
	traceLog(func() {
		var firstEventID int64 = -1
		if response.History != nil && len(response.History.Events) > 0 {
			firstEventID = response.History.Events[0].GetEventId()
		}
		wtp.logger.Debug("workflowTaskPoller::Poll Succeed",
			"StartedEventID", response.GetStartedEventId(),
			"Attempt", response.GetAttempt(),
			"FirstEventID", firstEventID,
			"IsQueryTask", response.Query != nil)
	})

	// 按 WorkflowType 打标签上报指标
	metricsHandler := wtp.metricsHandler.WithTags(metrics.WorkflowTags(response.WorkflowType.GetName()))
	metricsHandler.Counter(metrics.WorkflowTaskQueuePollSucceedCounter).Inc(1)

	// 记录调度延迟：ScheduledTime → StartedTime（Server 内部等待+分发的耗时）
	scheduleToStartLatency := response.GetStartedTime().AsTime().Sub(response.GetScheduledTime().AsTime())
	metricsHandler.Timer(metrics.WorkflowTaskScheduleToStartLatency).Record(scheduleToStartLatency)
	return task, nil
}

func (wtp *workflowTaskPoller) toWorkflowTask(response *workflowservice.PollWorkflowTaskQueueResponse) *workflowTask {
	return &workflowTask{
		task: response,
		historyIterator: &retrievingHistoryIterator{
			inner: &historyIteratorImpl{
				execution:      response.WorkflowExecution,
				nextPageToken:  response.NextPageToken,
				namespace:      wtp.namespace,
				service:        wtp.service,
				maxEventID:     response.GetStartedEventId(),
				metricsHandler: wtp.metricsHandler,
				taskQueue:      wtp.taskQueueName,
			},
			inboundVisitor:            wtp.inboundPayloadVisitor,
			payloadVisitorConcurrency: wtp.payloadVisitorConcurrency,
		},
	}
}

func (wtp *workflowTaskProcessor) toWorkflowTask(response *workflowservice.PollWorkflowTaskQueueResponse) *workflowTask {
	return &workflowTask{
		task: response,
		historyIterator: &retrievingHistoryIterator{
			inner: &historyIteratorImpl{
				execution:      response.WorkflowExecution,
				nextPageToken:  response.NextPageToken,
				namespace:      wtp.namespace,
				service:        wtp.service,
				maxEventID:     response.GetStartedEventId(),
				metricsHandler: wtp.metricsHandler,
				taskQueue:      wtp.taskQueueName,
			},
			inboundVisitor:            wtp.inboundPayloadVisitor,
			payloadVisitorConcurrency: wtp.payloadVisitorConcurrency,
		},
	}
}

func (h *historyIteratorImpl) GetNextPage() (*historypb.History, error) {
	if h.iteratorFunc == nil {
		h.iteratorFunc = newGetHistoryPageFunc(
			context.Background(),
			h.service,
			h.namespace,
			h.execution,
			h.maxEventID,
			h.metricsHandler,
			h.taskQueue,
		)
	}

	history, token, err := h.iteratorFunc(h.nextPageToken)
	if err != nil {
		return nil, err
	}
	h.nextPageToken = token
	return history, nil
}

func (h *historyIteratorImpl) Reset() {
	h.nextPageToken = nil
}

func (h *historyIteratorImpl) HasNextPage() bool {
	return h.nextPageToken != nil
}

func (r *retrievingHistoryIterator) GetNextPage() (*historypb.History, error) {
	history, err := r.inner.GetNextPage()
	if err != nil || history == nil {
		return history, err
	}
	if err := visitProtoPayloads(context.Background(), r.inboundVisitor, history, r.payloadVisitorConcurrency); err != nil {
		return nil, err
	}
	return history, nil
}

func (r *retrievingHistoryIterator) HasNextPage() bool { return r.inner.HasNextPage() }
func (r *retrievingHistoryIterator) Reset()            { r.inner.Reset() }

func newGetHistoryPageFunc(
	ctx context.Context,
	service workflowservice.WorkflowServiceClient,
	namespace string,
	execution *commonpb.WorkflowExecution,
	lastEventID int64,
	metricsHandler metrics.Handler,
	taskQueue string,
) func(nextPageToken []byte) (*historypb.History, []byte, error) {
	return func(nextPageToken []byte) (*historypb.History, []byte, error) {
		var resp *workflowservice.GetWorkflowExecutionHistoryResponse
		grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(
			metricsHandler.WithTags(metrics.RPCTags(metrics.NoneTagValue, metrics.NoneTagValue, taskQueue))),
			defaultGrpcRetryParameters(ctx))
		defer cancel()

		resp, err := service.GetWorkflowExecutionHistory(grpcCtx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     namespace,
			Execution:     execution,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return nil, nil, err
		}

		var h *historypb.History

		if resp.RawHistory != nil {
			h, err = serializer.DeserializeBlobDataToHistoryEvents(resp.RawHistory, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			if err != nil {
				return nil, nil, nil
			}
		} else {
			h = resp.History
		}

		size := len(h.Events)
		// While the SDK is processing a workflow task, the workflow task could timeout and server would start
		// a new workflow task or the server looses the workflow task if it is a speculative workflow task. In either
		// case, the new workflow task could have events that are beyond the last event ID that the SDK expects to process.
		// In such cases, the SDK should return error indicating that the workflow task is stale since the result will not be used.
		if size > 0 && lastEventID > 0 &&
			h.Events[size-1].GetEventId() > lastEventID {
			return nil, nil, fmt.Errorf("history contains events past expected last event ID (%v) "+
				"likely this means the current workflow task is no longer valid", lastEventID)

		}

		return h, resp.NextPageToken, nil
	}
}

func newActivityTaskPoller(taskHandler ActivityTaskHandler, service workflowservice.WorkflowServiceClient, params workerExecutionParameters) *activityTaskPoller {
	return &activityTaskPoller{
		basePoller: basePoller{
			metricsHandler:               params.MetricsHandler,
			stopC:                        params.WorkerStopChannel,
			workerBuildID:                params.getBuildID(),
			useBuildIDVersioning:         params.UseBuildIDForVersioning,
			workerDeploymentVersion:      params.DeploymentOptions.Version,
			capabilities:                 params.capabilities,
			pollTimeTracker:              params.pollTimeTracker,
			workerInstanceKey:            params.workerInstanceKey,
			workerPollCompleteOnShutdown: params.workerPollCompleteOnShutdown,
		},
		taskHandler:         taskHandler,
		service:             service,
		namespace:           params.Namespace,
		taskQueueName:       params.TaskQueue,
		identity:            params.Identity,
		logger:              params.Logger,
		activitiesPerSecond: params.TaskQueueActivitiesPerSecond,
		numPollerMetric:     newNumPollerMetric(params.MetricsHandler, metrics.PollerTypeActivityTask),
	}
}

// Poll the activity task queue and update the num_poller metric
func (atp *activityTaskPoller) pollActivityTaskQueue(ctx context.Context, request *workflowservice.PollActivityTaskQueueRequest) (*workflowservice.PollActivityTaskQueueResponse, error) {
	atp.numPollerMetric.increment()
	defer atp.numPollerMetric.decrement()

	return atp.service.PollActivityTaskQueue(ctx, request)
}

// Poll for a single activity task from the service
func (atp *activityTaskPoller) poll(ctx context.Context) (taskForWorker, error) {
	traceLog(func() {
		atp.logger.Debug("activityTaskPoller::Poll")
	})
	request := &workflowservice.PollActivityTaskQueueRequest{
		Namespace:         atp.namespace,
		TaskQueue:         &taskqueuepb.TaskQueue{Name: atp.taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:          atp.identity,
		TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: wrapperspb.Double(atp.activitiesPerSecond)},
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId:              atp.workerBuildID,
			UseVersioning:        atp.useBuildIDVersioning,
			DeploymentSeriesName: atp.getDeploymentName(),
		},
		DeploymentOptions: workerDeploymentOptionsToProto(
			atp.useBuildIDVersioning,
			atp.workerDeploymentVersion,
		),
		WorkerInstanceKey: atp.workerInstanceKey,
	}

	response, err := atp.pollActivityTaskQueue(ctx, request)
	if err != nil {
		return nil, err
	}
	if response == nil || len(response.TaskToken) == 0 {
		// No activity info is available on empty poll.  Emit using base scope.
		atp.metricsHandler.Counter(metrics.ActivityPollNoTaskCounter).Inc(1)
		return &activityTask{}, nil
	}

	atp.pollTimeTracker.recordPollSuccess(metrics.PollerTypeActivityTask)

	workflowType := response.WorkflowType.GetName()
	activityType := response.ActivityType.GetName()
	metricsHandler := atp.metricsHandler.WithTags(metrics.ActivityTags(workflowType, activityType, atp.taskQueueName))

	scheduleToStartLatency := response.GetStartedTime().AsTime().Sub(response.GetCurrentAttemptScheduledTime().AsTime())
	metricsHandler.Timer(metrics.ActivityScheduleToStartLatency).Record(scheduleToStartLatency)

	return &activityTask{task: response}, nil
}

// PollTask polls a new task
func (atp *activityTaskPoller) PollTask() (taskForWorker, error) {
	// Get the task.
	activityTask, err := atp.doPoll(atp.poll)
	if err != nil {
		return nil, err
	}
	return activityTask, nil
}

// ProcessTask processes a new task
func (atp *activityTaskPoller) ProcessTask(task interface{}) error {
	if !atp.shouldDrainOnShutdown() && atp.stopping() {
		return errStop
	}

	activityTask := task.(*activityTask)
	if activityTask.task == nil {
		// We didn't have task, poll might have timeout.
		traceLog(func() {
			atp.logger.Debug("Activity task unavailable")
		})
		return nil
	}

	workflowType := activityTask.task.WorkflowType.GetName()
	activityType := activityTask.task.ActivityType.GetName()
	activityMetricsHandler := atp.metricsHandler.WithTags(metrics.ActivityTags(workflowType, activityType, atp.taskQueueName))

	executionStartTime := time.Now()

	// Process the activity task.
	request, err := atp.taskHandler.Execute(atp.taskQueueName, activityTask.task)

	// err is returned in case of internal failure, such as unable to propagate context or context timeout.
	if err != nil {
		activityMetricsHandler.Counter(metrics.ActivityExecutionFailedCounter).Inc(1)
		return err
	}

	// in case if activity execution failed, request should be of type RespondActivityTaskFailedRequest
	if req, ok := request.(*workflowservice.RespondActivityTaskFailedRequest); ok {
		if !isBenignProtoApplicationFailure(req.Failure) {
			activityMetricsHandler.Counter(metrics.ActivityExecutionFailedCounter).Inc(1)
		}
	}
	activityMetricsHandler.Timer(metrics.ActivityExecutionLatency).Record(time.Since(executionStartTime))

	if request == ErrActivityResultPending {
		return nil
	}

	rpcMetricsHandler := atp.metricsHandler.WithTags(metrics.RPCTags(workflowType, activityType, metrics.NoneTagValue))
	reportErr := reportActivityComplete(context.Background(), atp.service, request, rpcMetricsHandler)
	if reportErr != nil {
		traceLog(func() {
			atp.logger.Debug("reportActivityComplete failed", tagError, reportErr)
		})
		return reportErr
	}

	if _, ok := request.(*workflowservice.RespondActivityTaskCompletedRequest); ok {
		activityMetricsHandler.
			Timer(metrics.ActivitySucceedEndToEndLatency).
			Record(time.Since(activityTask.task.GetScheduledTime().AsTime()))
	}
	return nil
}

func reportActivityComplete(
	ctx context.Context,
	service workflowservice.WorkflowServiceClient,
	request interface{},
	rpcMetricsHandler metrics.Handler,
) error {
	if request == nil {
		// nothing to report
		return nil
	}

	var reportErr error
	switch rqst := request.(type) {
	case *workflowservice.RespondActivityTaskCanceledRequest:
		grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(rpcMetricsHandler),
			defaultGrpcRetryParameters(ctx))
		defer cancel()
		_, err := service.RespondActivityTaskCanceled(grpcCtx, rqst)
		reportErr = err
	case *workflowservice.RespondActivityTaskFailedRequest:
		grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(rpcMetricsHandler), defaultGrpcRetryParameters(ctx))
		defer cancel()
		_, err := service.RespondActivityTaskFailed(grpcCtx, rqst)
		reportErr = err
	case *workflowservice.RespondActivityTaskCompletedRequest:
		grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(rpcMetricsHandler),
			defaultGrpcRetryParameters(ctx))
		defer cancel()
		_, err := service.RespondActivityTaskCompleted(grpcCtx, rqst)
		reportErr = err
	}
	return reportErr
}

func reportActivityCompleteByID(
	ctx context.Context,
	service workflowservice.WorkflowServiceClient,
	request interface{},
	rpcMetricsHandler metrics.Handler,
) error {
	if request == nil {
		// nothing to report
		return nil
	}

	var reportErr error
	switch request := request.(type) {
	case *workflowservice.RespondActivityTaskCanceledByIdRequest:
		grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(rpcMetricsHandler),
			defaultGrpcRetryParameters(ctx))
		defer cancel()
		_, err := service.RespondActivityTaskCanceledById(grpcCtx, request)
		reportErr = err
	case *workflowservice.RespondActivityTaskFailedByIdRequest:
		grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(rpcMetricsHandler),
			defaultGrpcRetryParameters(ctx))
		defer cancel()
		_, err := service.RespondActivityTaskFailedById(grpcCtx, request)
		reportErr = err
	case *workflowservice.RespondActivityTaskCompletedByIdRequest:
		grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(rpcMetricsHandler),
			defaultGrpcRetryParameters(ctx))
		defer cancel()
		_, err := service.RespondActivityTaskCompletedById(grpcCtx, request)
		reportErr = err
	}
	return reportErr
}

func convertActivityResultToRespondRequest(
	identity string,
	taskToken []byte,
	result *commonpb.Payloads,
	err error,
	dataConverter converter.DataConverter,
	failureConverter converter.FailureConverter,
	namespace string,
	cancelAllowed bool,
	versionStamp *commonpb.WorkerVersionStamp,
	deployment *deploymentpb.Deployment,
	workerDeploymentOptions *deploymentpb.WorkerDeploymentOptions,
) interface{} {
	if err == ErrActivityResultPending {
		// activity result is pending and will be completed asynchronously.
		// nothing to report at this point
		return ErrActivityResultPending
	}

	if err == nil {
		return &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken:         taskToken,
			Result:            result,
			Identity:          identity,
			Namespace:         namespace,
			WorkerVersion:     versionStamp,
			Deployment:        deployment,
			DeploymentOptions: workerDeploymentOptions,
		}
	}

	// Only respond with canceled if allowed
	if cancelAllowed {
		var canceledErr *CanceledError
		if errors.As(err, &canceledErr) {
			return &workflowservice.RespondActivityTaskCanceledRequest{
				TaskToken:         taskToken,
				Details:           convertErrDetailsToPayloads(canceledErr.details, dataConverter),
				Identity:          identity,
				Namespace:         namespace,
				WorkerVersion:     versionStamp,
				Deployment:        deployment,
				DeploymentOptions: workerDeploymentOptions,
			}
		}
		if errors.Is(err, context.Canceled) {
			return &workflowservice.RespondActivityTaskCanceledRequest{
				TaskToken:         taskToken,
				Identity:          identity,
				Namespace:         namespace,
				WorkerVersion:     versionStamp,
				Deployment:        deployment,
				DeploymentOptions: workerDeploymentOptions,
			}
		}
	}

	// If a canceled error is returned but it wasn't allowed, we have to wrap in
	// an unexpected-cancel application error
	if _, isCanceledErr := err.(*CanceledError); isCanceledErr {
		err = fmt.Errorf("unexpected activity cancel error: %w", err)
	}

	return &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken:         taskToken,
		Failure:           failureConverter.ErrorToFailure(err),
		Identity:          identity,
		Namespace:         namespace,
		WorkerVersion:     versionStamp,
		Deployment:        deployment,
		DeploymentOptions: workerDeploymentOptions,
	}
}

func convertActivityResultToRespondRequestByID(
	identity string,
	namespace string,
	workflowID string,
	runID string,
	activityID string,
	result *commonpb.Payloads,
	err error,
	dataConverter converter.DataConverter,
	failureConverter converter.FailureConverter,
	cancelAllowed bool,
) interface{} {
	if err == ErrActivityResultPending {
		// activity result is pending and will be completed asynchronously.
		// nothing to report at this point
		return nil
	}

	if err == nil {
		return &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  namespace,
			WorkflowId: workflowID,
			RunId:      runID,
			ActivityId: activityID,
			Result:     result,
			Identity:   identity,
		}
	}

	// Only respond with canceled if allowed
	if cancelAllowed {
		var canceledErr *CanceledError
		if errors.As(err, &canceledErr) {
			return &workflowservice.RespondActivityTaskCanceledByIdRequest{
				Namespace:  namespace,
				WorkflowId: workflowID,
				RunId:      runID,
				ActivityId: activityID,
				Details:    convertErrDetailsToPayloads(canceledErr.details, dataConverter),
				Identity:   identity,
			}
		}
		if errors.Is(err, context.Canceled) {
			return &workflowservice.RespondActivityTaskCanceledByIdRequest{
				Namespace:  namespace,
				WorkflowId: workflowID,
				RunId:      runID,
				ActivityId: activityID,
				Identity:   identity,
			}
		}
	}

	// If a canceled error is returned but it wasn't allowed, we have to wrap in
	// an unexpected-cancel application error
	if _, isCanceledErr := err.(*CanceledError); isCanceledErr {
		err = fmt.Errorf("unexpected activity cancel error: %w", err)
	}

	return &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:  namespace,
		WorkflowId: workflowID,
		RunId:      runID,
		ActivityId: activityID,
		Failure:    failureConverter.ErrorToFailure(err),
		Identity:   identity,
	}
}

func (wft *workflowTask) isEmpty() bool {
	return wft.task == nil
}

func (wft *workflowTask) scaleDecision() (pollerScaleDecision, bool) {
	if wft.task == nil || wft.task.PollerScalingDecision == nil {
		return pollerScaleDecision{}, false
	}
	return pollerScaleDecision{
		pollRequestDeltaSuggestion: int(wft.task.PollerScalingDecision.PollRequestDeltaSuggestion),
	}, true
}

func (at *activityTask) isEmpty() bool {
	return at.task == nil
}

func (at *activityTask) scaleDecision() (pollerScaleDecision, bool) {
	if at.task == nil || at.task.PollerScalingDecision == nil {
		return pollerScaleDecision{}, false
	}
	return pollerScaleDecision{
		pollRequestDeltaSuggestion: int(at.task.PollerScalingDecision.PollRequestDeltaSuggestion),
	}, true
}

func (*localActivityTask) isEmpty() bool {
	return false
}

func (*localActivityTask) scaleDecision() (pollerScaleDecision, bool) {
	return pollerScaleDecision{}, false
}

func (*eagerWorkflowTask) isEmpty() bool {
	return false
}

func (*eagerWorkflowTask) scaleDecision() (pollerScaleDecision, bool) {
	return pollerScaleDecision{}, false
}

func (nt *nexusTask) isEmpty() bool {
	return nt.task == nil
}

func (nt *nexusTask) scaleDecision() (pollerScaleDecision, bool) {
	if nt.task == nil || nt.task.PollerScalingDecision == nil {
		return pollerScaleDecision{}, false
	}
	return pollerScaleDecision{
		pollRequestDeltaSuggestion: int(nt.task.PollerScalingDecision.PollRequestDeltaSuggestion),
	}, true
}

// commandAwarePayloadVisitor is a wrapper around a PayloadVisitor that adds command-specific context information
type commandAwarePayloadVisitor struct {
	innerVisitor PayloadVisitor
	workflowInfo *WorkflowInfo
}

var _ PayloadVisitorWithContextHook = (*commandAwarePayloadVisitor)(nil)

func (v *commandAwarePayloadVisitor) Visit(ctx *proxy.VisitPayloadsContext, payload []*commonpb.Payload) ([]*commonpb.Payload, error) {
	if v.innerVisitor == nil {
		return payload, nil
	}
	return v.innerVisitor.Visit(ctx, payload)
}

func (v *commandAwarePayloadVisitor) ContextHook(ctx context.Context, msg proto.Message) (context.Context, error) {
	switch attrs := msg.(type) {
	case *commandpb.StartChildWorkflowExecutionCommandAttributes:
		ctx = extstore.WithStorageTarget(ctx, converter.StorageDriverWorkflowInfo{
			Namespace:    v.workflowInfo.Namespace,
			WorkflowType: attrs.WorkflowType.GetName(),
			WorkflowID:   attrs.WorkflowId,
		})
	case *commandpb.SignalExternalWorkflowExecutionCommandAttributes:
		ctx = extstore.WithStorageTarget(ctx, converter.StorageDriverWorkflowInfo{
			Namespace:  v.workflowInfo.Namespace,
			WorkflowID: attrs.Execution.GetWorkflowId(),
			RunID:      attrs.Execution.GetRunId(),
		})
	case *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes:
		// The new run keeps the same workflow ID. WorkflowType comes from the
		// command if specified (type change), otherwise falls back to the current
		// type already in context. RunID is omitted — the new run hasn't started.
		wfType := attrs.WorkflowType.GetName()
		if wfType == "" {
			wfType = v.workflowInfo.WorkflowType.Name
		}
		ctx = extstore.WithStorageTarget(ctx, converter.StorageDriverWorkflowInfo{
			Namespace:    v.workflowInfo.Namespace,
			WorkflowID:   v.workflowInfo.WorkflowExecution.ID,
			WorkflowType: wfType,
		})
	case *commandpb.CompleteWorkflowExecutionCommandAttributes:
		// Set target to parent context if not a continue-as-new workflow
		if v.workflowInfo.ParentWorkflowExecution != nil && v.workflowInfo.ContinuedExecutionRunID == "" {
			ns := v.workflowInfo.ParentWorkflowNamespace
			if ns == "" {
				ns = v.workflowInfo.Namespace
			}
			ctx = extstore.WithStorageTarget(ctx, converter.StorageDriverWorkflowInfo{
				Namespace:  ns,
				WorkflowID: v.workflowInfo.ParentWorkflowExecution.ID,
				RunID:      v.workflowInfo.ParentWorkflowExecution.RunID,
			})
		}
	}

	if innerVisitorWithHook, ok := v.innerVisitor.(PayloadVisitorWithContextHook); ok {
		return innerVisitorWithHook.ContextHook(ctx, msg)
	}

	return ctx, nil
}
