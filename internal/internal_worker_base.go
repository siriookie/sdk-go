package internal

// All code in this file is private to the package.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.temporal.io/sdk/internal/common/retry"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/backoff"
	"go.temporal.io/sdk/internal/common/metrics"
	internallog "go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/log"
)

const (
	retryPollOperationInitialInterval         = 200 * time.Millisecond
	retryPollOperationMaxInterval             = 10 * time.Second
	retryPollResourceExhaustedInitialInterval = time.Second
	retryPollResourceExhaustedMaxInterval     = 10 * time.Second
	// How long the same poll task error can remain suppressed
	lastPollTaskErrSuppressTime     = 1 * time.Minute
	pollerAutoscalingReportInterval = 100 * time.Millisecond
)

var (
	pollOperationRetryPolicy         = createPollRetryPolicy()
	pollResourceExhaustedRetryPolicy = createPollResourceExhaustedRetryPolicy()
	retryLongPollGracePeriod         = 2 * time.Minute
	errStop                          = errors.New("worker stopping")
	// ErrWorkerStopped is returned when the worker is stopped
	//
	// Exposed as: [go.temporal.io/sdk/worker.ErrWorkerShutdown]
	ErrWorkerShutdown = errors.New("worker is now shutdown")
)

type (
	// ResultHandler that returns result
	ResultHandler func(result *commonpb.Payloads, err error)
	// LocalActivityResultHandler that returns local activity result
	LocalActivityResultHandler func(lar *LocalActivityResultWrapper)

	// LocalActivityResultWrapper contains the result of a local activity
	LocalActivityResultWrapper struct {
		Err     error
		Result  *commonpb.Payloads
		Attempt int32
		Backoff time.Duration
	}

	LocalActivityMarkerParams struct {
		Summary string
	}

	ExecuteNexusOperationParams struct {
		client      NexusClient
		operation   string
		input       *commonpb.Payload
		options     NexusOperationOptions
		nexusHeader map[string]string
	}
)

// NewExecuteNexusOperationParams builds a parameters struct for
// WorkflowEnvironment.ExecuteNexusOperation. Exposed so that non-Go SDKs
// (e.g. roadrunner-temporal proxying for PHP) can populate the struct from
// outside the `internal` package — the fields themselves stay unexported to
// keep the SDK free to evolve them.
func NewExecuteNexusOperationParams(
	client NexusClient,
	operation string,
	input *commonpb.Payload,
	options NexusOperationOptions,
	nexusHeader map[string]string,
) ExecuteNexusOperationParams {
	return ExecuteNexusOperationParams{
		client:      client,
		operation:   operation,
		input:       input,
		options:     options,
		nexusHeader: nexusHeader,
	}
}

type (

	// WorkflowEnvironment Represents the environment for workflow.
	// Should only be used within the scope of workflow definition.
	WorkflowEnvironment interface {
		AsyncActivityClient
		LocalActivityClient
		WorkflowTimerClient
		SideEffect(f func() (*commonpb.Payloads, error), callback ResultHandler, summary string)
		GetVersion(changeID string, minSupported, maxSupported Version) Version
		WorkflowInfo() *WorkflowInfo
		TypedSearchAttributes() SearchAttributes
		Complete(result *commonpb.Payloads, err error)
		RegisterCancelHandler(handler func())
		RequestCancelChildWorkflow(namespace, workflowID string)
		RequestCancelExternalWorkflow(namespace, workflowID, runID string, callback ResultHandler)
		ExecuteChildWorkflow(params ExecuteWorkflowParams, callback ResultHandler, startedHandler func(r WorkflowExecution, e error))
		ExecuteNexusOperation(params ExecuteNexusOperationParams, callback func(*commonpb.Payload, error), startedHandler func(token string, e error)) int64
		RequestCancelNexusOperation(seq int64)
		GetLogger() log.Logger
		GetMetricsHandler() metrics.Handler
		// Must be called before WorkflowDefinition.Execute returns
		RegisterSignalHandler(
			handler func(name string, input *commonpb.Payloads, header *commonpb.Header) error,
		)
		SignalExternalWorkflow(
			namespace string,
			workflowID string,
			runID string,
			signalName string,
			input *commonpb.Payloads,
			arg interface{},
			header *commonpb.Header,
			childWorkflowOnly bool,
			callback ResultHandler,
		)
		RegisterQueryHandler(
			handler func(queryType string, queryArgs *commonpb.Payloads, header *commonpb.Header) (*commonpb.Payloads, error),
		)
		RegisterUpdateHandler(
			handler func(string, string, *commonpb.Payloads, *commonpb.Header, UpdateCallbacks),
		)
		IsReplaying() bool
		MutableSideEffect(id string, f func() interface{}, equals func(a, b interface{}) bool, summary string) converter.EncodedValue
		GetDataConverter() converter.DataConverter
		GetFailureConverter() converter.FailureConverter
		AddSession(sessionInfo *SessionInfo)
		RemoveSession(sessionID string)
		GetContextPropagators() []ContextPropagator
		UpsertSearchAttributes(attributes map[string]interface{}) error
		UpsertTypedSearchAttributes(attributes SearchAttributes) error
		UpsertMemo(memoMap map[string]interface{}) error
		GetRegistry() *registry
		// QueueUpdate request of type name
		QueueUpdate(name string, f func())
		// HandleQueuedUpdates unblocks all queued updates of type name
		HandleQueuedUpdates(name string)
		// DrainUnhandledUpdates unblocks all updates, meant to be used to drain
		// all unhandled updates at the end of a workflow task
		// returns true if any update was unblocked
		DrainUnhandledUpdates() bool
		// TryUse returns true if this flag may currently be used.
		TryUse(flag sdkFlag) bool
		GenerateSequence() int64
	}

	// WorkflowDefinitionFactory factory for creating WorkflowDefinition instances.
	WorkflowDefinitionFactory interface {
		// NewWorkflowDefinition must return a new instance of WorkflowDefinition on each call.
		NewWorkflowDefinition() WorkflowDefinition
	}

	// WorkflowDefinition wraps the code that can execute a workflow.
	WorkflowDefinition interface {
		// Execute implementation must be asynchronous.
		Execute(env WorkflowEnvironment, header *commonpb.Header, input *commonpb.Payloads)
		// OnWorkflowTaskStarted is called for each non timed out startWorkflowTask event.
		// Executed after all history events since the previous commands are applied to WorkflowDefinition
		// Application level code must be executed from this function only.
		// Execute call as well as callbacks called from WorkflowEnvironment functions can only schedule callbacks
		// which can be executed from OnWorkflowTaskStarted().
		OnWorkflowTaskStarted(deadlockDetectionTimeout time.Duration)
		// StackTrace of all coroutines owned by the Dispatcher instance.
		StackTrace() string
		// Close destroys all coroutines without waiting for their completion
		Close()
	}

	scalableTaskPoller struct {
		taskPollerType string
		// pollerCount is the number of pollers tasks to start. There may be less than this
		// due to limited slots, rate limiting, or poller autoscaling.
		pollerCount                  int
		taskPoller                   taskPoller
		pollerAutoscalerReportHandle *pollScalerReportHandle
		pollerSemaphore              *pollerSemaphore
	}

	// baseWorkerOptions options to configure base worker.
	baseWorkerOptions struct {
		pollerRate                   int
		slotSupplier                 SlotSupplier
		maxTaskPerSecond             float64
		taskPollers                  []scalableTaskPoller
		taskProcessor                taskProcessor
		workerType                   string
		identity                     string
		buildId                      string
		deploymentOptions            WorkerDeploymentOptions
		logger                       log.Logger
		stopTimeout                  time.Duration
		fatalErrCb                   func(error)
		backgroundContextCancel      context.CancelCauseFunc
		metricsHandler               metrics.Handler
		sessionTokenBucket           *sessionTokenBucket
		slotReservationData          slotReservationData
		isInternalWorker             bool
		workerPollCompleteOnShutdown *atomic.Bool
	}

	// baseWorker that wraps worker activities.
	baseWorker struct {
		options              baseWorkerOptions
		isWorkerStarted      bool
		stopCh               chan struct{}  // Channel used to stop the go routines.
		stopWG               sync.WaitGroup // The WaitGroup for stopping existing routines.
		pollLimiter          *rate.Limiter
		taskLimiter          *rate.Limiter
		limiterContext       context.Context
		limiterContextCancel func()
		// taskLimiterContext stays live during shutdown drain so already-polled
		// tasks still respect the dispatch rate after poll-side waits are
		// canceled via limiterContext.
		taskLimiterContext       context.Context
		taskLimiterContextCancel func()
		retrier                  *backoff.ConcurrentRetrier // Service errors back off retrier
		logger                   log.Logger
		metricsHandler           metrics.Handler

		slotSupplier       *trackingSlotSupplier
		taskQueueCh        chan eagerOrPolledTask
		eagerTaskQueueCh   chan eagerTask
		fatalErrCb         func(error)
		sessionTokenBucket *sessionTokenBucket
		pollerBalancer     *pollerBalancer

		lastPollTaskErrMessage string
		lastPollTaskErrStarted time.Time
		lastPollTaskErrLock    sync.Mutex

		noRepoll atomic.Bool
		pollerWG sync.WaitGroup
	}

	eagerOrPolledTask interface {
		getTask() taskForWorker
		getPermit() *SlotPermit
	}

	polledTask struct {
		task   taskForWorker
		permit *SlotPermit
	}

	eagerTask struct {
		// task to process.
		task   taskForWorker
		permit *SlotPermit
	}

	pollScalerReportHandleOptions struct {
		initialPollerCount        int
		maxPollerCount            int
		minPollerCount            int
		logger                    log.Logger
		scaleCallback             func(int)
		serverSupportsAutoscaling *atomic.Bool
	}

	pollScalerReportHandle struct {
		minPollerCount            int
		maxPollerCount            int
		logger                    log.Logger
		target                    atomic.Int64
		scaleCallback             func(int)
		everSawScalingDecision    atomic.Bool
		serverSupportsAutoscaling *atomic.Bool
		ingestedThisPeriod        atomic.Int64
		ingestedLastPeriod        atomic.Int64
		scaleUpAllowed            atomic.Bool
	}

	barrier chan struct{}

	// pollerSemaphore is a semaphore that limits the number of concurrent pollers.
	// it is effectively a resizable semaphore.
	pollerSemaphore struct {
		maxPermits int
		permits    int
		bs         chan barrier
	}

	// pollerBalancer is used to balance the number of poll requests from different poller types
	pollerBalancer struct {
		pollerCount   map[string]int
		pollerBarrier map[string]barrier
		mu            sync.Mutex
	}
)

func (h ResultHandler) wrap(callback ResultHandler) ResultHandler {
	return func(result *commonpb.Payloads, err error) {
		callback(result, err)
		h(result, err)
	}
}

func (t *polledTask) getTask() taskForWorker {
	return t.task
}
func (t *polledTask) getPermit() *SlotPermit {
	return t.permit
}
func (t *eagerTask) getTask() taskForWorker {
	return t.task
}
func (t *eagerTask) getPermit() *SlotPermit {
	return t.permit
}

// SetRetryLongPollGracePeriod sets the amount of time a long poller retries on
// fatal errors before it actually fails. For test use only,
// not safe to call with a running worker.
func SetRetryLongPollGracePeriod(period time.Duration) {
	retryLongPollGracePeriod = period
}

func getRetryLongPollGracePeriod() time.Duration {
	return retryLongPollGracePeriod
}

func createPollRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryPollOperationInitialInterval)
	policy.SetMaximumInterval(retryPollOperationMaxInterval)

	// NOTE: We don't use expiration interval since we don't use retries from retrier class.
	// We use it to calculate next backoff. We have additional layer that is built on poller
	// in the worker layer for to add some middleware for any poll retry that includes
	// (a) rate limiting across pollers (b) back-off across pollers when server is busy
	policy.SetExpirationInterval(retry.UnlimitedInterval) // We don't ever expire
	return policy
}

func createPollResourceExhaustedRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryPollResourceExhaustedInitialInterval)
	policy.SetMaximumInterval(retryPollResourceExhaustedMaxInterval)
	policy.SetExpirationInterval(retry.UnlimitedInterval)
	return policy
}

func newBaseWorker(
	options baseWorkerOptions,
) *baseWorker {
	ctx, cancel := context.WithCancel(context.Background())
	taskLimiterCtx, taskLimiterCancel := context.WithCancel(context.Background())
	logger := log.With(options.logger, tagWorkerType, options.workerType)
	if heartbeatHandler, isHeartbeat := options.metricsHandler.(*heartbeatMetricsHandler); isHeartbeat {
		options.metricsHandler = heartbeatHandler.forWorker(options.workerType)
	}
	metricsHandler := options.metricsHandler.WithTags(metrics.WorkerTags(options.workerType))
	tss := newTrackingSlotSupplier(options.slotSupplier, trackingSlotSupplierOptions{
		logger:         logger,
		metricsHandler: metricsHandler,
		workerBuildId:  options.buildId,
		workerIdentity: options.identity,
	})
	bw := &baseWorker{
		options:        options,
		stopCh:         make(chan struct{}),
		taskLimiter:    rate.NewLimiter(rate.Limit(options.maxTaskPerSecond), 1),
		retrier:        backoff.NewConcurrentRetrier(pollOperationRetryPolicy),
		logger:         logger,
		metricsHandler: metricsHandler,

		slotSupplier: tss,
		// No buffer, so pollers are only able to poll for new tasks after the previous one is
		// dispatched.
		taskQueueCh: make(chan eagerOrPolledTask),
		// Allow enough capacity so that eager dispatch will not block. There's an upper limit of
		// 2k pending activities so this channel never needs to be larger than that.
		eagerTaskQueueCh: make(chan eagerTask, 2000),
		fatalErrCb:       options.fatalErrCb,

		limiterContext:           ctx,
		limiterContextCancel:     cancel,
		taskLimiterContext:       taskLimiterCtx,
		taskLimiterContextCancel: taskLimiterCancel,
		sessionTokenBucket:       options.sessionTokenBucket,
	}
	// Set secondary retrier as resource exhausted
	bw.retrier.SetSecondaryRetryPolicy(pollResourceExhaustedRetryPolicy)
	if options.pollerRate > 0 {
		bw.pollLimiter = rate.NewLimiter(rate.Limit(options.pollerRate), 1)
	}
	// If we have multiple task workers, we need to balance the pollers
	if len(options.taskPollers) > 1 {
		bw.pollerBalancer = &pollerBalancer{
			pollerCount:   make(map[string]int),
			pollerBarrier: make(map[string]barrier),
		}
	}

	return bw
}

// Start starts a fixed set of routines to do the work.
// Start 启动 baseWorker，创建所有后台 goroutine。
//
// baseWorker 是 workflowWorker / activityWorker / nexusWorker 的共同基类，
// 封装了 poller 管理和任务分发的通用逻辑。每种具体 Worker 的 Start() 最终
// 都会调用到 baseWorker.Start()。
//
// 此函数做的事情（纯本地，无 gRPC）：
//  1. 按配置启动 N 个 poller goroutine（每个在 runPoller 中做长轮询）
//  2. 启动 autoscaler goroutine（如启用，用于动态调整 poller 数量）
//  3. 启动 taskDispatcher goroutine（从 taskQueueCh 取任务 → 分发执行）
//  4. 启动 eagerTaskDispatcher goroutine（处理 Eager Activity 任务）
//
// 所有 goroutine 都通过 stopCh（关闭时通知退出）+ stopWG（等待全部结束）来
// 实现优雅停止。
func (bw *baseWorker) Start() {
	// 防止重复启动：如果已经启动过，直接返回（幂等）
	if bw.isWorkerStarted {
		return
	}

	bw.metricsHandler.Counter(metrics.WorkerStartCounter).Inc(1)

	// ===== 1. 启动 Poller goroutines =====
	// taskPollers 是一个 []scalableTaskPoller 列表，每个 scalableTaskPoller 代表一种
	// 任务类型的 poller 配置。例如 workflowWorker 只有一个 WorkflowTask poller，
	// 而 workflowWorker 内部的 localActivityWorker 也有自己的 poller。
	for _, taskWorker := range bw.options.taskPollers {
		// 注册到 pollerBalancer（用于在多种任务类型间动态平衡 poller 资源）
		if bw.pollerBalancer != nil {
			bw.pollerBalancer.registerPollerType(taskWorker.taskPollerType)
		}

		// 创建 pollerCount 个 goroutine，每个 goroutine 独立长轮询
		// runPoller 的核心逻辑：Poll → 收到 Task → 放到 taskQueueCh → 再 Poll
		for i := 0; i < taskWorker.pollerCount; i++ {
			bw.stopWG.Add(1)
			bw.pollerWG.Add(1)
			go bw.runPoller(taskWorker)
		}

		// 自动扩缩容 handle：独立 goroutine 定时向 Server 上报负载，
		// 并根据 Server 返回的决策调整 poller 数量
		if taskWorker.pollerAutoscalerReportHandle != nil {
			bw.stopWG.Add(1)
			go func() {
				defer bw.stopWG.Done()
				taskWorker.pollerAutoscalerReportHandle.run(bw.stopCh)
			}()
		}
	}

	// When all pollers have exited, close taskQueueCh so the dispatcher
	// knows no more polled tasks will arrive and can drain what remains.
	bw.stopWG.Add(1)
	go func() {
		defer bw.stopWG.Done()
		bw.pollerWG.Wait()
		close(bw.taskQueueCh)
	}()

	// ===== 2. 启动任务分发器（核心事件循环）=====
	// runTaskDispatcher 是 Worker 的"主事件循环"，它从 taskQueueCh 读取任务，
	// 做限流，然后调用 processTaskAsync 提交给 slotSupplier 执行。
	// 限流逻辑：polledTask 受 taskLimiter 限速；eagerTask（本地 Activity 结果等）不限速。
	bw.stopWG.Add(1)
	go bw.runTaskDispatcher()

	// ===== 3. 启动 Eager Activity 分发器 =====
	// runEagerTaskDispatcher 处理 Server 通过 RespondWorkflowTaskCompleted Response
	// 直接下发的 Eager Activity Task（跳过 Poll 环节，降低延迟）。
	bw.stopWG.Add(1)
	go bw.runEagerTaskDispatcher()

	bw.isWorkerStarted = true
	traceLog(func() {
		bw.logger.Info("Started Worker",
			"MaxTaskPerSecond", bw.options.maxTaskPerSecond,
		)
	})
}

func (bw *baseWorker) isStop() bool {
	select {
	case <-bw.stopCh:
		return true
	default:
		return false
	}
}

func (bw *baseWorker) shouldDrainOnShutdown() bool {
	return bw.options.workerPollCompleteOnShutdown != nil && bw.options.workerPollCompleteOnShutdown.Load()
}

// runPoller 是 Worker 的"心跳循环"——每个 poller goroutine 的核心函数。
//
// 整个函数是一个无限的 for{} 循环，每次循环代表一次"抢槽位 + 长轮询 + 处理任务"的完整周期。
//
// 关键设计：槽位预约（ReserveSlot）在独立 goroutine 中异步执行，主 poller goroutine 通过
// select{stopCh, reserveChan} 同时监听"退出信号"和"槽位就绪"，确保任何时候都能被 Stop() 中断。
func (bw *baseWorker) runPoller(taskWorker scalableTaskPoller) {
	defer bw.stopWG.Done()
	defer bw.pollerWG.Done()
	// Note: With poller autoscaling, this metric doesn't make a lot of sense since the number of pollers can go up and down.
	bw.metricsHandler.Counter(metrics.PollerStartCounter).Inc(1)

	// ctx 用于取消正在进行的 ReserveSlot 异步操作
	ctx, cancelfn := context.WithCancel(context.Background())
	defer cancelfn()
	// reserveChan 是 ReserveSlot 异步结果的"回传通道"
	reserveChan := make(chan *SlotPermit)

	for {
		// 整个循环体被包装成一个闭包，返回 true 表示需要退出 for 循环
		if func() bool {
			// ===== 步骤 1：前置检查 =====

			// noRepoll：Stop() 时设置为 true，阻止新一轮 Poll（防止 Stop 和 Poll 之间的竞态）
			if bw.noRepoll.Load() {
				return true
			}
			// Poller 信号量：限制同时进行 Poll 的 goroutine 数量
			// acquire 阻塞直到获取到信号量或 ctx 被取消（Stop 会触发 cancel）
			if taskWorker.pollerSemaphore != nil {
				if taskWorker.pollerSemaphore.acquire(bw.limiterContext) != nil {
					return true
				}
				defer taskWorker.pollerSemaphore.release()
			}
			// Poller 负载均衡器：确保一种任务类型的 poller 不会饿死其他类型
			// balance 阻塞直到此 poller 类型被允许继续或 ctx 被取消
			if bw.pollerBalancer != nil {
				if bw.pollerBalancer.balance(bw.limiterContext, taskWorker.taskPollerType) != nil {
					return true
				}
			}

			// ===== 步骤 2：异步预约执行槽位 =====
			// ReserveSlot 可能需要排队等待（比如达到 MaxConcurrent 上限），
			// 所以必须在独立的 goroutine 中执行——否则主 poller 会在等待槽位时
			// 无法响应 stopCh 退出信号。
			bw.stopWG.Add(1)
			go func() {
				defer bw.stopWG.Done()
				s, err := bw.slotSupplier.ReserveSlot(ctx, &bw.options.slotReservationData)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						bw.logger.Error("Error while trying to reserve slot", "error", err)
						// 槽位预约失败 → 通过 reserveChan 发 nil 通知主循环
						select {
						case reserveChan <- nil:
						case <-ctx.Done():
							return
						}
					}
					return
				}
				// 槽位预约成功 → 将 permit 发回主循环
				select {
				case reserveChan <- s:
				case <-ctx.Done():
					// ctx 被取消（Stop 时触发）→ 释放刚拿到的槽位
					bw.releaseSlot(s, SlotReleaseReasonUnused)
				}
			}()

			// ===== 步骤 3：等待槽位或退出信号 =====
			select {
			case <-bw.stopCh:
				// Worker 正在停止 → 退出。异步 ReserveSlot goroutine 会因 ctx.Done() 而清理。
				return true
			case permit := <-reserveChan:
				if permit == nil {
					// 槽位预约失败 → 短暂休眠避免疯狂重试
					if ctx.Err() == nil {
						time.Sleep(time.Second)
					}
					return false // 不退出循环，下一轮再试
				}

				// ===== 步骤 4：执行 Poll → 处理 Task =====
				// Session Token Bucket 限流（仅 Session Worker 使用）
				if bw.sessionTokenBucket != nil {
					bw.sessionTokenBucket.waitForAvailableToken()
				}
				// 通知负载均衡器：此类型的 poller 进入活跃状态
				if bw.pollerBalancer != nil {
					bw.pollerBalancer.incrementPoller(taskWorker.taskPollerType)
				}

				// pollTask 是单次 Poll → 处理的完整过程：
				//   长轮询 gRPC → 收到 Task → 放入 taskQueueCh → 等待完成 → 释放槽位
				bw.pollTask(taskWorker, permit)

				// Poll 完成 → 通知负载均衡器此 poller 变为空闲
				if bw.pollerBalancer != nil {
					bw.pollerBalancer.decrementPoller(taskWorker.taskPollerType)
				}
			}
			return false // 一轮完成，继续下一轮循环
		}() {
			return // 闭包返回 true → 退出外层 for 循环
		}
	}
}

func (bw *baseWorker) tryReserveSlot() *SlotPermit {
	if bw.isStop() {
		return nil
	}
	return bw.slotSupplier.TryReserveSlot(&bw.options.slotReservationData)
}

func (bw *baseWorker) releaseSlot(permit *SlotPermit, reason SlotReleaseReason) {
	bw.slotSupplier.ReleaseSlot(permit, reason)
}

func (bw *baseWorker) pushEagerTask(task eagerTask) {
	// Should always be non-blocking. Slots are reserved before requesting eager tasks.
	bw.eagerTaskQueueCh <- task
}

func (bw *baseWorker) getDeploymentOptions() WorkerDeploymentOptions {
	return bw.options.deploymentOptions
}

func (bw *baseWorker) processTaskAsync(eagerOrPolled eagerOrPolledTask) {
	// 这个 task 即将交给新的 goroutine 异步处理。
	// stopWG 用来让 Worker Stop 时等待这些已经开始处理的 task 收尾。
	bw.stopWG.Add(1)
	// 每个 task 单独起一个 goroutine 处理。
	// dispatcher 本身不会同步执行任务，否则一个慢 task 会阻塞后续 task 分发。
	go func() {
		// goroutine 退出时通知 stopWG：这个 task 的异步处理结束了。
		defer bw.stopWG.Done()

		// 从 eagerOrPolled 包装里取出真正的 task。
		// task 可能是 workflow task、activity task、local activity result、nexus task 等。
		task := eagerOrPolled.getTask()
		// 取出这个 task 关联的 slot permit。
		// permit 是 poll 前或 eager task 创建前预留的执行容量凭证。
		permit := eagerOrPolled.getPermit()

		// 空 task 表示一次 poll 成功返回但没有实际任务。
		// 非空 task 才算真正占用执行槽位，需要把 reserved slot 标记为 used。
		if !task.isEmpty() {
			bw.slotSupplier.MarkSlotUsed(permit)
		}

		// 无论任务处理成功、失败、panic，都要释放 slot。
		// 这样 worker 不会因为某个 task 异常导致执行容量永久泄漏。
		defer func() {
			// task 已经完成处理，释放 permit。
			bw.releaseSlot(permit, SlotReleaseReasonTaskProcessed)

			// 兜底捕获 task processor 的 panic。
			// workflow/activity 业务代码或 SDK 处理逻辑 panic 时，不让整个 worker goroutine 崩掉。
			if p := recover(); p != nil {
				// 生成当前 goroutine 的 stack trace，便于定位 panic 来源。
				topLine := "base worker [panic]:"
				st := getStackTraceRaw(topLine, 7, 0)
				// 记录 panic 值和堆栈。
				bw.logger.Error("Unhandled panic.",
					"PanicError", fmt.Sprintf("%v", p),
					"PanicStack", st)
			}
		}()
		// 调用具体任务处理器。
		// 对 workflow worker，这里会进入 workflowTaskProcessor.ProcessTask；
		// 对 activity worker，这里会进入 activityTaskPoller.ProcessTask；
		// baseWorker 不关心任务类型，只通过 taskProcessor 接口分发。
		err := bw.options.taskProcessor.ProcessTask(task)
		if err != nil {
			// client side error 通常是 SDK/业务侧可预期错误，按 Info 记录。
			if isClientSideError(err) {
				bw.logger.Info("Task processing failed with client side error", tagError, err)
			} else {
				// 其它处理错误也记录，但这里不直接停止 worker；
				// 具体失败如何上报 server，通常由对应 taskProcessor 内部完成。
				bw.logger.Info("Task processing failed with error", tagError, err)
			}
		}
	}()
}

func (bw *baseWorker) runTaskDispatcher() {
	defer bw.stopWG.Done()

	if bw.shouldDrainOnShutdown() {
		for task := range bw.taskQueueCh {
			// For non-polled-task (local activity result as task or eager task),
			// we don't need to rate limit. Keep using the dispatch limiter during
			// shutdown drain so already-polled tasks still respect the worker's
			// configured task start rate.
			if _, isPolledTask := task.(*polledTask); isPolledTask {
				if bw.taskLimiter.Wait(bw.taskLimiterContext) != nil {
					bw.releaseSlot(task.getPermit(), SlotReleaseReasonUnused)
					// Pollers in drain mode send to taskQueueCh without a
					// stopCh escape hatch. Keep receiving and releasing any
					// remaining tasks so pollers can finish and taskQueueCh can
					// close even after we stop processing tasks.
					bw.discardTaskQueue()
					return
				}
			}
			bw.processTaskAsync(task)
		}
		return
	}

	for {
		select {
		case <-bw.stopCh:
			return
		case task, ok := <-bw.taskQueueCh:
			if !ok {
				return
			}
			if _, isPolledTask := task.(*polledTask); isPolledTask {
				if bw.taskLimiter.Wait(bw.limiterContext) != nil {
					if bw.isStop() {
						bw.releaseSlot(task.getPermit(), SlotReleaseReasonUnused)
						return
					}
				}
			}
			bw.processTaskAsync(task)
		}
	}
}

func (bw *baseWorker) discardTaskQueue() {
	for task := range bw.taskQueueCh {
		bw.releaseSlot(task.getPermit(), SlotReleaseReasonUnused)
	}
}

func (bw *baseWorker) runEagerTaskDispatcher() {
	defer bw.stopWG.Done()
	for {
		select {
		case <-bw.stopCh:
			// drain eager dispatch queue
			for len(bw.eagerTaskQueueCh) > 0 {
				eagerTask := <-bw.eagerTaskQueueCh
				bw.processTaskAsync(&eagerTask)
			}
			return
		case eagerTask := <-bw.eagerTaskQueueCh:
			bw.processTaskAsync(&eagerTask)
		}
	}
}

// pollTask 执行一次完整的"长轮询 → 处理"流程。
//
// 被 runPoller 在拿到槽位后调用，是 Worker 中唯一直连 Server gRPC 的函数。
//
// 流程：
//  1. retrier.Throttle    — 如果上次 Poll 失败，按退避策略等待
//  2. pollLimiter         — 限速检查（控制 Poll 频率）
//  3. PollTask() gRPC     — 🔴 长轮询，可能阻塞几十秒等待 Server 返回 Task
//  4. 错误处理            — 不可重试错误+宽限期超期 → 致命错误关闭 Worker
//  5. 成功 → 通知 autoscaler → taskQueueCh <- polledTask → 交给 dispatcher
//
// 槽位管理：defer 保证如果 task 没成功入队到 dispatcher（didSendTask=false），
// 自动释放槽位。成功入队后槽位由 processTaskAsync 在执行完成时释放。
func (bw *baseWorker) pollTask(taskWorker scalableTaskPoller, slotPermit *SlotPermit) {
	var err error
	var task taskForWorker
	didSendTask := false
	// defer: 如果 task 没有被成功送入 taskQueueCh（如 Poll 出错、stopCh 触发），
	// 自动释放槽位。成功入队不释放——槽位随 task 一起交给 dispatcher。
	defer func() {
		if !didSendTask {
			bw.releaseSlot(slotPermit, SlotReleaseReasonUnused)
		}
	}()

	// ===== 步骤 1：退避等待 =====
	// retrier 记录上次 Poll 是否失败。如果失败过，Throttle 会阻塞等待退避时间。
	// stopCh 可以打断等待（Stop() 时不会卡住）。
	bw.retrier.Throttle(bw.stopCh)

	// ===== 步骤 2 + 3：限速 + 长轮询 =====
	// pollLimiter 是可选的（pollerRate > 0 时启用），限制 Poll 频率防止打爆 Server。
	if bw.pollLimiter == nil || bw.pollLimiter.Wait(bw.limiterContext) == nil {
		// 🔴 核心 gRPC 调用：长轮询等待 Server 下发 Task
		task, err = taskWorker.taskPoller.PollTask()
		// 统一日志处理（相同错误 30 秒内只打一次 WARN）
		bw.logPollTaskError(err)
		if err != nil {
			// ===== 步骤 4a：Poll 报错 =====
			// 不可重试错误（如 NamespaceNotFound、ClientVersionNotSupported）
			// 在"宽限期"内仍会重试（代理层可能临时返回异常值），
			// 超过宽限期后触发致命错误关闭整个 Worker。
			if isNonRetriableError(err) && bw.retrier.GetElapsedTime() > getRetryLongPollGracePeriod() {
				bw.logger.Error("Worker received non-retriable error. Shutting down.", tagError, err)
				if bw.fatalErrCb != nil {
					bw.fatalErrCb(err)
				}
				return
			}
			// 通知 autoscaler：出错了，可能需要缩容
			if taskWorker.pollerAutoscalerReportHandle != nil {
				taskWorker.pollerAutoscalerReportHandle.handleError(err)
			}
			// 标记失败 → 下次 Throttle 会等待退避
			// ResourceExhausted 用第二档退避策略（更激进的等待）
			_, resourceExhausted := err.(*serviceerror.ResourceExhausted)
			bw.retrier.Failed(resourceExhausted)
		} else {
			// ===== 步骤 4b：Poll 成功 =====
			bw.retrier.Succeeded() // 重置重试计数器
		}
	}

	// ===== 步骤 5：将 Task 送入 dispatcher =====
	if task != nil {
		// 通知 autoscaler：拿到了一个 task（可能是空的）
		if taskWorker.pollerAutoscalerReportHandle != nil {
			taskWorker.pollerAutoscalerReportHandle.handleTask(task)
		}

	if bw.shouldDrainOnShutdown() {
		// The dispatcher is guaranteed to be alive: in drain mode it
		// only exits after taskQueueCh is closed, which happens after
		// all pollers finish.
		bw.taskQueueCh <- &polledTask{task: task, permit: slotPermit}
			didSendTask = true
		} else {
			select {
			case bw.taskQueueCh <- &polledTask{task: task, permit: slotPermit}:
				didSendTask = true
			case <-bw.stopCh:
			}
		}
	}
}

func (bw *baseWorker) logPollTaskError(err error) {
	// We do not want to log any errors after we were explicitly stopped
	select {
	case <-bw.stopCh:
		return
	default:
	}

	bw.lastPollTaskErrLock.Lock()
	defer bw.lastPollTaskErrLock.Unlock()
	// No error means reset the message and time
	if err == nil {
		bw.lastPollTaskErrMessage = ""
		bw.lastPollTaskErrStarted = time.Now()
		return
	}

	// Ignore connection loss on server shutdown. This helps with quiescing spurious error messages
	// upon server shutdown (where server is using the SDK).
	if bw.options.isInternalWorker {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Unavailable && strings.Contains(st.Message(), "graceful_stop") {
			return
		}
	}

	// Log the error as warn if it doesn't match the last error seen or its over
	// the time since
	if err.Error() != bw.lastPollTaskErrMessage || time.Since(bw.lastPollTaskErrStarted) > lastPollTaskErrSuppressTime {
		bw.logger.Warn("Failed to poll for task.", tagError, err)
		bw.lastPollTaskErrMessage = err.Error()
		bw.lastPollTaskErrStarted = time.Now()
	}
}

func isNonRetriableError(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *serviceerror.InvalidArgument,
		*serviceerror.NamespaceNotFound,
		*serviceerror.ClientVersionNotSupported:
		return true
	}
	return false
}

// Stop is a blocking call and cleans up all the resources associated with worker.
func (bw *baseWorker) Stop() {
	if !bw.isWorkerStarted {
		return
	}
	close(bw.stopCh)
	bw.limiterContextCancel()

	// Wait for pollers, task dispatch, and task processing to complete, or until stopTimeout elapses.
	// The task dispatcher drains taskQueueCh after the closer goroutine
	// closes it when pollers finish.
	if success := awaitWaitGroup(&bw.stopWG, bw.options.stopTimeout); !success {
		traceLog(func() {
			bw.logger.Info("Worker graceful stop timed out.", "Stop timeout", bw.options.stopTimeout)
		})
	}
	bw.taskLimiterContextCancel()

	// Close context
	if bw.options.backgroundContextCancel != nil {
		bw.options.backgroundContextCancel(ErrWorkerShutdown)
	}

	bw.isWorkerStarted = false
}

func (bw *baseWorker) stopPolling() {
	bw.noRepoll.Store(true)
}

func newPollScalerReportHandle(options pollScalerReportHandleOptions) *pollScalerReportHandle {
	logger := options.logger
	if logger == nil {
		logger = internallog.NewNopLogger()
	}
	serverSupportsAutoscaling := options.serverSupportsAutoscaling
	if serverSupportsAutoscaling == nil {
		serverSupportsAutoscaling = &atomic.Bool{}
	}
	psr := &pollScalerReportHandle{
		maxPollerCount:            options.maxPollerCount,
		minPollerCount:            options.minPollerCount,
		logger:                    logger,
		scaleCallback:             options.scaleCallback,
		serverSupportsAutoscaling: serverSupportsAutoscaling,
	}
	psr.target.Store(int64(options.initialPollerCount))
	return psr
}

func (prh *pollScalerReportHandle) handleTask(task taskForWorker) {
	// task.isEmpty() 表示这次 poll 没拿到真正任务。
	// workflow/activity/nexus poller 在 server 返回空响应时，会构造一个 empty task。
	if !task.isEmpty() {
		// 非空 task 说明本周期内 worker 实际摄入了一个任务。
		// run()/newPeriod() 会周期性读取这个计数，用来判断最近是否还有吞吐。
		prh.ingestedThisPeriod.Add(1)
	}

	// 从 task 里读取 server 返回的 PollerScalingDecision。
	// 目前 workflow/activity/nexus task 会从 poll response 的 PollerScalingDecision 字段转换出这个值；
	// local activity/eager workflow task 没有 server poll response，所以通常没有 scaling decision。
	if sd, ok := task.scaleDecision(); ok {
		// 只要见过一次 server scaling decision，就记下来。
		// 后面即使遇到空 poll，也允许根据空 poll 缩容，避免 poller 一直保持过高数量。
		prh.everSawScalingDecision.Store(true)
		// pollRequestDeltaSuggestion 是 server 建议 SDK 调整的 poll 请求数量增量。
		// 正数表示建议增加 poller，负数表示建议减少 poller，0 表示不调整。
		ds := sd.pollRequestDeltaSuggestion
		if ds > 0 {
			// server 建议扩容时，SDK 还要看本地 scaleUpAllowed。
			// 这个开关由周期逻辑控制，避免在没有足够吞吐/信号时盲目增加 poller。
			if prh.scaleUpAllowed.Load() {
				// 原子更新目标 poller 数：target = target + ds。
				// updateTarget 内部会把结果限制在 min/maxPollerCount 范围内，并调用 scaleCallback。
				prh.updateTarget(func(target int64) int64 {
					return target + int64(ds)
				})
			}
		} else if ds < 0 {
			// server 建议缩容时直接执行，不受 scaleUpAllowed 限制。
			// 例如 Matching 看到空 poll、低负载或资源压力时，可以建议 worker 降低 poll 并发。
			prh.updateTarget(func(target int64) int64 {
				return target + int64(ds)
			})
		}
	} else if task.isEmpty() && (prh.everSawScalingDecision.Load() || prh.serverSupportsAutoscaling.Load()) {
		// We want to avoid scaling down on empty polls if the server has never made any
		// scaling decisions - otherwise we might never scale up again. If the server
		// supports poller autoscaling, it's safe to scale down without having seen a
		// decision.
		// 没有 server decision，但这次 poll 是空的：
		// 如果已经确认 server 支持 autoscaling，或者过去已经见过 server decision，
		// 就把空 poll 当作低负载信号，尝试把目标 poller 数减 1。
		prh.updateTarget(func(target int64) int64 {
			return target - 1
		})
	}
}

func (prh *pollScalerReportHandle) updateTarget(f func(int64) int64) {
	target := prh.target.Load()
	newTarget := f(target)
	if newTarget < int64(prh.minPollerCount) {
		newTarget = int64(prh.minPollerCount)
	} else if newTarget > int64(prh.maxPollerCount) {
		newTarget = int64(prh.maxPollerCount)
	}
	for !prh.target.CompareAndSwap(target, newTarget) {
		target = prh.target.Load()
		newTarget = f(target)
		if newTarget < int64(prh.minPollerCount) {
			newTarget = int64(prh.minPollerCount)
		} else if newTarget > int64(prh.maxPollerCount) {
			newTarget = int64(prh.maxPollerCount)
		}
	}
	permits := int(newTarget)
	if prh.scaleCallback != nil {
		traceLog(func() {
			prh.logger.Debug("Updating number of permits", "permits", permits)
		})
		prh.scaleCallback(permits)
	}
}

func (prh *pollScalerReportHandle) handleError(err error) {
	// If we have never seen a scaling decision and the server doesn't support
	// poller autoscaling, we don't want to scale down on errors, because we
	// might never scale up again.
	if prh.everSawScalingDecision.Load() || prh.serverSupportsAutoscaling.Load() {
		_, resourceExhausted := err.(*serviceerror.ResourceExhausted)
		if resourceExhausted {
			prh.updateTarget(func(target int64) int64 {
				return target / 2
			})
		} else {
			prh.updateTarget(func(target int64) int64 {
				return target - 1
			})
		}
	}
}

func (prh *pollScalerReportHandle) run(stopCh <-chan struct{}) {
	ticker := time.NewTicker(pollerAutoscalingReportInterval)
	// Here we periodically check if we should permit increasing the
	// poller count further. We do this by comparing the number of ingested items in the
	// current period with the number of ingested items in the previous period. If we
	// are successfully ingesting more items, then it makes sense to allow scaling up.
	// If we aren't, then we're probably limited by how fast we can process the tasks
	// and it's not worth increasing the poller count further.
	for {
		select {
		case <-ticker.C:
			prh.newPeriod()
		case <-stopCh:
			return
		}
	}
}

func (prh *pollScalerReportHandle) newPeriod() {
	ingestedThisPeriod := prh.ingestedThisPeriod.Swap(0)
	ingestedLastPeriod := prh.ingestedLastPeriod.Swap(ingestedThisPeriod)
	prh.scaleUpAllowed.Store(float64(ingestedThisPeriod) >= float64(ingestedLastPeriod)*1.1)
}

func newPollerSemaphore(maxPermits int) *pollerSemaphore {
	ps := &pollerSemaphore{
		maxPermits: maxPermits,
		permits:    0,
		bs:         make(chan barrier, 1),
	}
	ps.bs <- make(barrier)
	return ps
}

func (ps *pollerSemaphore) acquire(ctx context.Context) error {
	for {
		// Acquire barrier.
		b := <-ps.bs
		if ps.permits < ps.maxPermits {
			ps.permits++
			// Release barrier.
			ps.bs <- b
			return nil
		}
		// Release barrier.
		ps.bs <- b

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b:
			continue
		}
	}
}

func (ps *pollerSemaphore) release() {
	// Acquire barrier.
	b := <-ps.bs
	ps.permits--
	// Release one waiter if there are any waiting.
	select {
	case b <- struct{}{}:
	default:
	}
	// Release barrier.
	ps.bs <- b
}

func (ps *pollerSemaphore) updatePermits(maxPermits int) {
	// Acquire barrier.
	b := <-ps.bs
	ps.maxPermits = maxPermits
	// Release barrier.
	ps.bs <- b
}

func newScalableTaskPoller(
	poller taskPoller,
	logger log.Logger,
	pollerBehavior PollerBehavior,
	taskPollerType string,
	serverSupportsAutoscaling *atomic.Bool,
) scalableTaskPoller {
	tw := scalableTaskPoller{
		taskPoller:     poller,
		taskPollerType: taskPollerType,
	}
	switch p := pollerBehavior.(type) {
	case *pollerBehaviorAutoscaling:
		tw.pollerCount = p.maximumNumberOfPollers
		tw.pollerSemaphore = newPollerSemaphore(p.initialNumberOfPollers)
		tw.pollerAutoscalerReportHandle = newPollScalerReportHandle(pollScalerReportHandleOptions{
			initialPollerCount:        p.initialNumberOfPollers,
			maxPollerCount:            p.maximumNumberOfPollers,
			minPollerCount:            p.minimumNumberOfPollers,
			logger:                    logger,
			serverSupportsAutoscaling: serverSupportsAutoscaling,
			scaleCallback: func(newTarget int) {
				tw.pollerSemaphore.updatePermits(newTarget)
			},
		})
	case *pollerBehaviorSimpleMaximum:
		tw.pollerCount = p.maximumNumberOfPollers
	}
	return tw
}

// balance checks if the poller type is balanced with other poller types. The goal is to ensure that
// at least one poller of each type is running before allowing any poller of the given type to increase.
// balance 在多 poller 类型之间做负载均衡，防止一种类型的 poller 饿死另一种。
//
// 背景：一个 Worker 可能有多种 poller 类型（如 WorkflowTaskPoller + ActivityTaskPoller），
// 它们共享有限的槽位。如果某种类型长期没有活跃 poller，其他类型的 poller 不应占满所有
// 槽位——否则那种"落后"的类型永远抢不到槽位，无法启动第一个 poller。
//
// 实现思路：
//
//	检查所有"其他"类型，如果某个类型的 pollerCount==0（没有一个活跃 poller），
//	当前 poller 就在该类型的 barrier 上阻塞等待。
//	当 incrementPoller() 被调用时（该类型终于有 poller 启动了），
//	如果 count 从 0→1，会 close(barrier) 叫醒所有等待中的其他类型 poller。
func (pb *pollerBalancer) balance(ctx context.Context, pollerType string) error {
	pb.mu.Lock()
	for {
		// 快速路径：如果自己连一个活跃 poller 都没有，那 polling 本身就不会再继续，
		// 不需要平衡检查。必须在遍历 map 前检查——否则 map 迭代顺序不确定，
		// 可能随机命中另一个类型并错误地阻塞。
		if pb.pollerCount[pollerType] <= 0 {
			pb.mu.Unlock()
			return nil
		}
		var b barrier
		// 遍历所有"其他"poller 类型：如果有任何一种 count==0，记录它的 barrier
		for pt, count := range pb.pollerCount {
			if pt == pollerType {
				continue
			}
			if count == 0 {
				b = pb.pollerBarrier[pt]
				break
			}
		}
		pb.mu.Unlock()
		// 所有其他类型都至少有一个活跃 poller → 平衡，放行
		if b == nil {
			return nil
		}
		// 存在某个其他类型没有活跃 poller → 在它的 barrier 上阻塞等待
		// 等 incrementPoller() 把该类型的 count 从 0→1 时 close(barrier) 叫醒我们
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b: // barrier 被 close 了 ← 那个类型终于启动了 poller
			pb.mu.Lock()
			continue // 重新检查是否还有别的不平衡
		}
	}
}

func (pb *pollerBalancer) registerPollerType(pollerType string) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if _, ok := pb.pollerCount[pollerType]; !ok {
		pb.pollerCount[pollerType] = 0
		pb.pollerBarrier[pollerType] = make(barrier)
	}
}

// incrementPoller 通知 balancer：此类型有一个 poller 进入活跃状态（开始 Poll）。
//
// 关键逻辑：当 count 从 0→1 时，close(barrier) 唤醒所有在 balance() 中阻塞的
// 其他类型 poller——"本类型终于有人了，你们可以继续了"。
// 然后立即创建一个新 barrier 替代，给后续可能的等待者使用。
// 利用 Go channel 特性：close(ch) → 所有 <-ch 立即返回零值（不阻塞）。
func (pb *pollerBalancer) incrementPoller(pollerType string) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.pollerCount[pollerType] == 0 {
		// count 0→1：此类型终于有人了！close 旧 barrier 叫醒所有等待者
		close(pb.pollerBarrier[pollerType])
		// 立即创建新 barrier，给后续可能需要等待的 poller 用
		pb.pollerBarrier[pollerType] = make(barrier)
	}
	pb.pollerCount[pollerType]++
}

// decrementPoller 通知 balancer：此类型有一个 poller 变为空闲（Poll 完成一轮）。
func (pb *pollerBalancer) decrementPoller(pollerType string) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.pollerCount[pollerType]--
}
