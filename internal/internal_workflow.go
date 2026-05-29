package internal

// All code in this file is private to the package.

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/sdk/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

const (
	defaultSignalChannelSize    = 100000 // really large buffering size(100K)
	defaultCoroutineExitTimeout = 100 * time.Millisecond

	panicIllegalAccessCoroutineState = "getState: illegal access from outside of workflow context"
	unhandledUpdateWarningMessage    = "[TMPRL1102] Workflow finished while update handlers are still running. This may have interrupted work that the" +
		" update handler was doing, and the client that sent the update will receive a 'workflow execution" +
		" already completed' RPCError instead of the update result. You can wait for all update" +
		" handlers to complete by using `workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })`. Alternatively, if both you and the clients sending the update" +
		" are okay with interrupting running handlers when the workflow finishes, and causing clients to" +
		" receive errors, then you can disable this warning via UnfinishedPolicy in UpdateHandlerOptions."
)

type (
	syncWorkflowDefinition struct {
		workflow   workflow
		dispatcher dispatcher
		cancel     CancelFunc
		rootCtx    Context
	}

	workflowResult struct {
		workflowResult *commonpb.Payloads
		error          error
	}

	futureImpl struct {
		value   interface{}
		err     error
		ready   bool
		channel *channelImpl
		chained []asyncFuture // Futures that are chained to this one
	}

	// Implements WaitGroup interface
	waitGroupImpl struct {
		n        int      // the number of coroutines to wait on
		waiting  bool     // indicates whether WaitGroup.Wait() has been called yet for the WaitGroup
		future   Future   // future to signal that all awaited members of the WaitGroup have completed
		settable Settable // used to unblock the future when all coroutines have completed
	}

	// Implements Mutex interface
	mutexImpl struct {
		locked bool
	}

	// Implements Semaphore interface
	semaphoreImpl struct {
		size int64
		cur  int64
	}

	// Dispatcher is a container of a set of coroutines.
	dispatcher interface {
		// ExecuteUntilAllBlocked executes coroutines one by one in deterministic order
		// until all of them are completed or blocked on Channel or Selector or timeout is reached.
		ExecuteUntilAllBlocked(deadlockDetectionTimeout time.Duration) (err error)
		// IsDone returns true when all of coroutines are completed
		IsDone() bool
		IsClosed() bool
		IsExecuting() bool
		Close()             // Destroys all coroutines without waiting for their completion
		StackTrace() string // Stack trace of all coroutines owned by the Dispatcher instance

		// NewCoroutine creates a new coroutine. To be called from within another coroutine.
		// Used by the interceptors.
		NewCoroutine(ctx Context, name string, highPriority bool, f func(ctx Context)) Context
	}

	// Workflow is an interface that any workflow should implement.
	// Code of a workflow must be deterministic. It must use workflow.Channel, workflow.Selector, and workflow.Go instead of
	// native channels, select and go. It also must not use range operation over map as it is randomized by go runtime.
	// All time manipulation should use current time returned by GetTime(ctx) method.
	// Note that workflow.Context is used instead of context.Context to avoid use of raw channels.
	workflow interface {
		Execute(ctx Context, input *commonpb.Payloads) (result *commonpb.Payloads, err error)
	}

	sendCallback struct {
		value interface{}
		fn    func() bool // false indicates that callback didn't accept the value
	}

	receiveCallback struct {
		// false result means that callback didn't accept the value and it is still up for delivery
		fn func(v interface{}, more bool) bool
	}

	channelImpl struct {
		name            string                  // human readable channel name
		size            int                     // Channel buffer size. 0 for non buffered.
		buffer          []interface{}           // buffered messages
		blockedSends    []*sendCallback         // puts waiting when buffer is full.
		blockedReceives []*receiveCallback      // receives waiting when no messages are available.
		closed          bool                    // true if channel is closed.
		recValue        *interface{}            // Used only while receiving value, this is used as pre-fetch buffer value from the channel.
		dataConverter   converter.DataConverter // for decode data
		env             WorkflowEnvironment
	}

	// Single case statement of the Select
	selectCase struct {
		channel     *channelImpl                       // Channel of this case.
		receiveFunc *func(c ReceiveChannel, more bool) // function to call when channel has a message. nil for send case.

		sendFunc   *func()         // function to call when channel accepted a message. nil for receive case.
		sendValue  *interface{}    // value to send to the channel. Used only for send case.
		future     asyncFuture     // Used for future case
		futureFunc *func(f Future) // function to call when Future is ready
	}

	// Implements Selector interface
	selectorImpl struct {
		name        string
		cases       []*selectCase // cases that this select is comprised from
		defaultFunc *func()       // default case
	}

	// unblockFunc is passed evaluated by a coroutine yield. When it returns false the yield returns to a caller.
	// stackDepth is the depth of stack from the last blocking call relevant to user.
	// Used to truncate internal stack frames from thread stack.
	unblockFunc func(status string, stackDepth int) (keepBlocked bool)

	coroutineState struct {
		name         string
		dispatcher   *dispatcherImpl  // dispatcher this context belongs to
		aboutToBlock chan bool        // used to notify dispatcher that coroutine that owns this context is about to block
		unblock      chan unblockFunc // used to notify coroutine that it should continue executing.
		keptBlocked  bool             // true indicates that coroutine didn't make any progress since the last yield unblocking
		closed       atomic.Bool      // indicates that owning coroutine has finished execution
		blocked      atomic.Bool
		panicError   error // non nil if coroutine had unhandled panic
	}

	dispatcherImpl struct {
		sequence         int
		channelSequence  int // used to name channels
		selectorSequence int // used to name channels
		coroutines       []*coroutineState
		executing        bool       // currently running ExecuteUntilAllBlocked. Used to avoid recursive calls to it.
		mutex            sync.Mutex // used to synchronize executing
		closed           bool
		interceptor      WorkflowOutboundInterceptor
		logger           log.Logger
		deadlockDetector *deadlockDetector
		readOnly         bool
		// allBlockedCallback is called when all coroutines are blocked,
		// returns true if the callback updated any coroutines state and there may be more work
		allBlockedCallback func() bool
		newEagerCoroutines []*coroutineState
	}

	// WorkflowOptions options passed to the workflow function
	// The current timeout resolution implementation is in seconds and uses math.Ceil() as the duration. But is
	// subjected to change in the future.
	WorkflowOptions struct {
		TaskQueueName            string
		WorkflowExecutionTimeout time.Duration
		WorkflowRunTimeout       time.Duration
		WorkflowTaskTimeout      time.Duration
		Namespace                string
		WorkflowID               string
		WaitForCancellation      bool
		WorkflowIDReusePolicy    enumspb.WorkflowIdReusePolicy
		// WorkflowIDConflictPolicy and OnConflictOptions are only used in test environment for
		// running Nexus operations as child workflow.
		WorkflowIDConflictPolicy enumspb.WorkflowIdConflictPolicy
		OnConflictOptions        *OnConflictOptions
		DataConverter            converter.DataConverter
		RetryPolicy              *commonpb.RetryPolicy
		Priority                 *commonpb.Priority
		CronSchedule             string
		ContextPropagators       []ContextPropagator
		Memo                     map[string]interface{}
		SearchAttributes         map[string]interface{}
		TypedSearchAttributes    SearchAttributes
		ParentClosePolicy        enumspb.ParentClosePolicy
		StaticSummary            string
		StaticDetails            string
		signalChannels           map[string]Channel
		requestedSignalChannels  map[string]*requestedSignalChannel
		queryHandlers            map[string]*queryHandler
		updateHandlers           map[string]*updateHandler
		// runningUpdatesHandles is a map of update handlers that are currently running.
		runningUpdatesHandles     map[string]UpdateInfo
		VersioningIntent          VersioningIntent
		InitialVersioningBehavior ContinueAsNewVersioningBehavior
		// currentDetails is the user-set string returned on metadata query as
		// WorkflowMetadata.current_details
		currentDetails string
	}

	// ExecuteWorkflowParams parameters of the workflow invocation
	ExecuteWorkflowParams struct {
		WorkflowOptions
		WorkflowType         *WorkflowType
		Input                *commonpb.Payloads
		Header               *commonpb.Header
		dataConverter        converter.DataConverter    // context-aware DC from ExecuteChildWorkflow
		failureConverter     converter.FailureConverter // context-aware FC from ExecuteChildWorkflow
		attempt              int32                      // used by test framework to support child workflow retry
		scheduledTime        time.Time                  // used by test framework to support child workflow retry
		lastCompletionResult *commonpb.Payloads         // used by test framework to support cron
	}

	// decodeFutureImpl
	decodeFutureImpl struct {
		*futureImpl
		fn            interface{}
		dataConverter converter.DataConverter // optional: if set, used instead of ctx DC
	}

	childWorkflowFutureImpl struct {
		*decodeFutureImpl             // for child workflow result
		executionFuture   *futureImpl // for child workflow execution future
	}

	nexusOperationFutureImpl struct {
		*decodeFutureImpl             // for the result
		executionFuture   *futureImpl // for the NexusOperationExecution
	}

	asyncFuture interface {
		Future
		// Used by selectorImpl
		// If Future is ready returns its value immediately.
		// If not registers callback which is called when it is ready.
		GetAsync(callback *receiveCallback) (v interface{}, ok bool, err error)

		// Used by selectorImpl
		RemoveReceiveCallback(callback *receiveCallback)

		// This future will added to list of dependency futures.
		ChainFuture(f Future)

		// Gets the current value and error.
		// Make sure this is called once the future is ready.
		GetValueAndError() (v interface{}, err error)

		Set(value interface{}, err error)
	}

	requestedSignalChannel struct {
		options SignalChannelOptions
	}

	queryHandler struct {
		fn            interface{}
		queryType     string
		dataConverter converter.DataConverter
		options       QueryHandlerOptions
	}

	// updateSchedulerImpl adapts the coro dispatcher to the UpdateScheduler interface
	updateSchedulerImpl struct {
		dispatcher dispatcher
	}
)

const (
	workflowEnvironmentContextKey    = "workflowEnv"
	workflowInterceptorContextKey    = "workflowInterceptor"
	localActivityFnContextKey        = "localActivityFn"
	workflowEnvInterceptorContextKey = "envInterceptor"
	workflowResultContextKey         = "workflowResult"
	coroutinesContextKey             = "coroutines"
	workflowEnvOptionsContextKey     = "wfEnvOptions"
	updateInfoContextKey             = "updateInfo"
)

// Assert that structs do indeed implement the interfaces
var _ Channel = (*channelImpl)(nil)
var _ Selector = (*selectorImpl)(nil)
var _ WaitGroup = (*waitGroupImpl)(nil)
var _ dispatcher = (*dispatcherImpl)(nil)

// 1MB buffer to fit combined stack trace of all active goroutines
var stackBuf [1024 * 1024]byte

var (
	errCoroStackNotFound   = errors.New("coroutine stack not found")
	errStackTraceTruncated = errors.New("stack trace truncated: stackBuf is too small")
)

// Pointer to pointer to workflow result
func 
(ctx Context) **workflowResult {
	rpp := ctx.Value(workflowResultContextKey)
	if rpp == nil {
		panic("getWorkflowResultPointerPointer: Not a workflow context")
	}
	return rpp.(**workflowResult)
}

func getWorkflowEnvironment(ctx Context) WorkflowEnvironment {
	wc := ctx.Value(workflowEnvironmentContextKey)
	if wc == nil {
		panic("getWorkflowContext: Not a workflow context")
	}
	return wc.(WorkflowEnvironment)
}

func getWorkflowEnvironmentInterceptor(ctx Context) *workflowEnvironmentInterceptor {
	wc := ctx.Value(workflowEnvInterceptorContextKey)
	if wc == nil {
		panic("getWorkflowContext: Not a workflow context")
	}
	return wc.(*workflowEnvironmentInterceptor)
}

type workflowEnvironmentInterceptor struct {
	env                 WorkflowEnvironment
	dispatcher          dispatcher
	inboundInterceptor  WorkflowInboundInterceptor
	fn                  interface{}
	outboundInterceptor WorkflowOutboundInterceptor
}

func (wc *workflowEnvironmentInterceptor) Go(ctx Context, name string, f func(ctx Context)) Context {
	return wc.dispatcher.NewCoroutine(ctx, name, false, f)
}

func getWorkflowOutboundInterceptor(ctx Context) WorkflowOutboundInterceptor {
	wc := ctx.Value(workflowInterceptorContextKey)
	if wc == nil {
		panic("getWorkflowOutboundInterceptor: Not a workflow context")
	}
	return wc.(WorkflowOutboundInterceptor)
}

func (f *futureImpl) Get(ctx Context, valuePtr interface{}) error {
	assertNotInReadOnlyState(ctx)
	more := f.channel.Receive(ctx, nil)
	if more {
		panic("not closed")
	}
	if !f.ready {
		panic("not ready")
	}
	if f.err != nil || f.value == nil || valuePtr == nil {
		return f.err
	}
	rf := reflect.ValueOf(valuePtr)
	if rf.Type().Kind() != reflect.Ptr {
		return errors.New("valuePtr parameter is not a pointer")
	}

	if payload, ok := f.value.(*commonpb.Payloads); ok {
		if _, ok2 := valuePtr.(**commonpb.Payloads); !ok2 {
			if err := decodeArg(getDataConverterFromWorkflowContext(ctx), payload, valuePtr); err != nil {
				return err
			}
			return f.err
		}
	}

	fv := reflect.ValueOf(f.value)
	// If the value set was a pointer and is the same type as the wanted result,
	// instead of panicking because it is not a pointer to a pointer, we will just
	// set the pointer
	if fv.Kind() == reflect.Ptr && fv.Type() == rf.Type() {
		rf.Elem().Set(fv.Elem())
	} else {
		rf.Elem().Set(fv)
	}
	return f.err
}

// Used by selectorImpl
// If Future is ready returns its value immediately.
// If not registers callback which is called when it is ready.
func (f *futureImpl) GetAsync(callback *receiveCallback) (v interface{}, ok bool, err error) {
	_, _, more := f.channel.receiveAsyncImpl(callback)
	// Future uses Channel.Close to indicate that it is ready.
	// So more being true (channel is still open) indicates future is not ready.
	if more {
		return nil, false, nil
	}
	if !f.ready {
		panic("not ready")
	}
	return f.value, true, f.err
}

// RemoveReceiveCallback removes the callback from future's channel to avoid closure leak.
// Used by selectorImpl
func (f *futureImpl) RemoveReceiveCallback(callback *receiveCallback) {
	f.channel.removeReceiveCallback(callback)
}

func (f *futureImpl) IsReady() bool {
	return f.ready
}

func (f *futureImpl) Set(value interface{}, err error) {
	if f.ready {
		panic("already set")
	}
	f.value = value
	f.err = err
	f.ready = true
	f.channel.Close()
	for _, ch := range f.chained {
		ch.Set(f.value, f.err)
	}
}

func (f *futureImpl) SetValue(value interface{}) {
	if f.ready {
		panic("already set")
	}
	f.Set(value, nil)
}

func (f *futureImpl) SetError(err error) {
	if f.ready {
		panic("already set")
	}
	f.Set(nil, err)
}

func (f *futureImpl) Chain(future Future) {
	if f.ready {
		panic("already set")
	}

	ch, ok := future.(asyncFuture)
	if !ok {
		panic("cannot chain Future that wasn't created with workflow.NewFuture")
	}
	if !ch.IsReady() {
		ch.ChainFuture(f)
		return
	}
	val, err := ch.GetValueAndError()
	f.Set(val, err)
}

func (f *futureImpl) ChainFuture(future Future) {
	f.chained = append(f.chained, future.(asyncFuture))
}

func (f *futureImpl) GetValueAndError() (interface{}, error) {
	return f.value, f.err
}

func (f *childWorkflowFutureImpl) GetChildWorkflowExecution() Future {
	return f.executionFuture
}

func (f *childWorkflowFutureImpl) SignalChildWorkflow(ctx Context, signalName string, data interface{}) Future {
	assertNotInReadOnlyState(ctx)
	var childExec WorkflowExecution
	if err := f.GetChildWorkflowExecution().Get(ctx, &childExec); err != nil {
		return f.GetChildWorkflowExecution()
	}

	i := getWorkflowOutboundInterceptor(ctx)
	// Put header on context before executing
	ctx = workflowContextWithNewHeader(ctx)
	return i.SignalChildWorkflow(ctx, childExec.ID, signalName, data)
}

func (f *nexusOperationFutureImpl) GetNexusOperationExecution() Future {
	return f.executionFuture
}

func newWorkflowContext(
	env WorkflowEnvironment,
	interceptors []WorkerInterceptor,
) (*workflowEnvironmentInterceptor, Context, error) {
	// Create context with default values
	ctx := WithValue(background, workflowEnvironmentContextKey, env)
	var resultPtr *workflowResult
	ctx = WithValue(ctx, workflowResultContextKey, &resultPtr)
	info := env.WorkflowInfo()
	ctx = WithWorkflowNamespace(ctx, info.Namespace)
	ctx = WithWorkflowTaskQueue(ctx, info.TaskQueueName)
	getWorkflowEnvOptions(ctx).WorkflowExecutionTimeout = info.WorkflowExecutionTimeout
	ctx = WithWorkflowRunTimeout(ctx, info.WorkflowRunTimeout)
	ctx = WithWorkflowTaskTimeout(ctx, info.WorkflowTaskTimeout)
	ctx = WithTaskQueue(ctx, info.TaskQueueName)
	ctx = WithDataConverter(ctx, env.GetDataConverter())
	ctx = withContextPropagators(ctx, env.GetContextPropagators())
	getActivityOptions(ctx).OriginalTaskQueueName = info.TaskQueueName

	// Create interceptor and put it on context as inbound and put it on context
	// as the default outbound interceptor before init
	envInterceptor := &workflowEnvironmentInterceptor{env: env}
	envInterceptor.inboundInterceptor = envInterceptor
	envInterceptor.outboundInterceptor = envInterceptor
	ctx = WithValue(ctx, workflowEnvInterceptorContextKey, envInterceptor)
	ctx = WithValue(ctx, workflowInterceptorContextKey, envInterceptor.outboundInterceptor)

	// Intercept, run init, and put the new outbound interceptor on the context
	for i := len(interceptors) - 1; i >= 0; i-- {
		envInterceptor.inboundInterceptor = interceptors[i].InterceptWorkflow(ctx, envInterceptor.inboundInterceptor)
	}
	err := envInterceptor.inboundInterceptor.Init(envInterceptor)
	if err != nil {
		return nil, nil, err
	}
	ctx = WithValue(ctx, workflowInterceptorContextKey, envInterceptor.outboundInterceptor)

	return envInterceptor, ctx, nil
}

func (d *syncWorkflowDefinition) Execute(env WorkflowEnvironment, header *commonpb.Header, input *commonpb.Payloads) {
	// 创建 workflow 根 Context 和 workflowEnvironmentInterceptor。
	// newWorkflowContext 会把 WorkflowEnvironment、workflow result 指针、namespace、task queue、
	// workflow/run/task timeout、DataConverter、ContextPropagators、Activity 默认 task queue 等写进 Context。
	// 它还会构造 envInterceptor，并按注册顺序反向包裹 WorkerInterceptor.InterceptWorkflow，
	// 最后调用 inboundInterceptor.Init，让 interceptor 有机会替换 outboundInterceptor。
	envInterceptor, rootCtx, err := newWorkflowContext(env, env.GetRegistry().interceptors)
	if err != nil {
		// Execute 接口没有 error 返回值；newWorkflowContext 初始化失败时只能 panic。
		// 外层 ProcessEvent 有 recover，会把 panic 转成 workflow task 失败/工作流 panic 错误路径。
		panic(err)
	}
	// 创建 workflow coroutine dispatcher，并创建名为 "root" 的根 coroutine。
	// newDispatcher 内部会把 envInterceptor.outboundInterceptor 放进 dispatcher，
	// 再调用 outboundInterceptor.Go(rootCtx, "root", rootFunc)，最终通过 dispatcher.NewCoroutine
	// 创建根 coroutine，并返回带 coroutinesContextKey 的 rootCtx。
	dispatcher, rootCtx := newDispatcher(
		rootCtx,
		envInterceptor,
		func(ctx Context) {
			// workflowResult 是 executeDispatcher 判断 workflow 是否已经返回的标记。
			// 只要 workflowResult 指针仍然是 nil，executeDispatcher 就认为 workflow 还在运行或阻塞中。
			r := &workflowResult{}

			// We want to execute the user workflow definition from the first workflow task started,
			// so they can see everything before that. Here we would have all initialization done, hence
			// we are yielding.
			// 这里主动 yield，保证 Execute 只是完成 runtime 初始化，不会立刻执行用户 workflow 函数。
			// 真正推进这个 root coroutine 的地方是 OnWorkflowTaskStarted -> executeDispatcher。
			// 因此 WorkflowExecutionStarted event 处理时会创建 coroutine，但用户 workflow 代码要等
			// WorkflowTaskStarted event 触发 dispatcher 才开始跑。
			state := getState(d.rootCtx)
			state.yield("yield before executing to setup state")
			// coroutine 被 dispatcher 再次调度回来后，标记当前 coroutine 已从 blocked/yield 状态恢复。
			state.unblocked()

			// 调用 d.workflow.Execute，也就是 getWorkflowDefinition 中创建的 workflowExecutor.Execute。
			// workflowExecutor.Execute 会按普通/dynamic workflow 的规则解码 input，
			// 经 inboundInterceptor.ExecuteWorkflow 调到用户 workflow 函数，
			// 再把用户返回值编码成 Payloads，错误原样返回。
			r.workflowResult, r.error = d.workflow.Execute(d.rootCtx, input)
			// ctx 是 newDispatcher 创建 root coroutine 时传入的 coroutine context，
			// 里面有 workflowResultContextKey，值是 **workflowResult。
			// 把结果写进去后，下一次 executeDispatcher 会看到 rp != nil，
			// 然后调用 env.Complete(result, err) 生成完成/失败/continue-as-new 等 workflow 结果。
			rpp := getWorkflowResultPointerPointer(ctx)
			*rpp = r
		}, getWorkflowEnvironment(rootCtx).DrainUnhandledUpdates)

	// set the information from the headers that is to be propagated in the workflow context
	// 把 WorkflowExecutionStarted event 上的 header 通过 ContextPropagator 注入 workflow Context。
	// workflowContextWithHeaderPropagated 会确保 header/header.Fields 非 nil，
	// 逐个调用 ContextPropagator.ExtractToWorkflow，然后把 header.Fields 存进 Context。
	rootCtx, err = workflowContextWithHeaderPropagated(rootCtx, header, env.GetContextPropagators())
	if err != nil {
		// header 传播失败同样无法从 Execute 返回 error，只能 panic，交给外层 workflow task 错误处理。
		panic(err)
	}

	// 给 root workflow context 加 cancellation 能力。
	// d.rootCtx 是后续 signal/update/query handler 和 workflowExecutor.Execute 使用的根 Context；
	// d.cancel 会被 cancel handler 调用，用来取消 workflow.Context。
	d.rootCtx, d.cancel = WithCancel(rootCtx)
	// 保存 dispatcher，后续 OnWorkflowTaskStarted 会调用 executeDispatcher(d.rootCtx, d.dispatcher, ...)
	// 来运行所有 ready coroutine，直到全部阻塞或 workflow 返回。
	d.dispatcher = dispatcher
	// envInterceptor.Go 需要 dispatcher.NewCoroutine；这里把 dispatcher 回填给 interceptor。
	// newDispatcher 里也设置过一次，这里确保后续 handler 使用的是同一个 dispatcher。
	envInterceptor.dispatcher = dispatcher

	// 注册 workflow cancellation 的入口。
	// ProcessEvent 处理 WorkflowExecutionCancelRequested 时会调用 workflowEnvironmentImpl.cancelHandler，
	// 这里注册的 handler 会调用 d.cancel()，让 workflow.Context 进入 canceled 状态。
	getWorkflowEnvironment(d.rootCtx).RegisterCancelHandler(func() {
		// It is ok to call this method multiple times.
		// it doesn't do anything new, the context remains canceled.
		d.cancel()
	})

	// 注册 signal 的入口。
	// ProcessEvent 处理 WorkflowExecutionSignaled event 时会调用 signalHandler(name,input,header)，
	// 最终进入这里，把 signal header 先传播到 workflow Context，
	// 再走 inboundInterceptor.HandleSignal。默认实现会把 payload 放进对应 signal channel。
	getWorkflowEnvironment(d.rootCtx).RegisterSignalHandler(
		func(name string, input *commonpb.Payloads, header *commonpb.Header) error {
			// Put the header on context
			rootCtx, err := workflowContextWithHeaderPropagated(d.rootCtx, header, env.GetContextPropagators())
			if err != nil {
				return err
			}
			return envInterceptor.inboundInterceptor.HandleSignal(rootCtx, &HandleSignalInput{SignalName: name, Arg: input})
		},
	)

	// 注册 update 的入口。
	// ProcessMessage 处理 Workflow Update protocol message 时会调用 updateHandler。
	// defaultUpdateHandler 会传播 header、查找用户 SetUpdateHandler 注册的 handler、
	// 解码 update 参数，并通过 updateSchedulerImpl{d.dispatcher} 创建/调度 update coroutine。
	getWorkflowEnvironment(d.rootCtx).RegisterUpdateHandler(
		func(name string, id string, serializedArgs *commonpb.Payloads, header *commonpb.Header, callbacks UpdateCallbacks) {
			defaultUpdateHandler(d.rootCtx, name, id, serializedArgs, header, callbacks, updateSchedulerImpl{d.dispatcher})
		})

	// 注册 query 的入口。
	// Query Task replay 完当前 history 后会调用 queryHandler(queryType,args,header)，
	// 最终进入这里。Query 不生成 commands，只读取 replay 后的 workflow 内存状态并返回序列化结果。
	getWorkflowEnvironment(d.rootCtx).RegisterQueryHandler(
		func(queryType string, queryArgs *commonpb.Payloads, header *commonpb.Header) (*commonpb.Payloads, error) {
			// Put the header on context if server supports it
			// 每次 query 都使用 query 自己携带的 header 传播到 workflow Context。
			rootCtx, err := workflowContextWithHeaderPropagated(d.rootCtx, header, env.GetContextPropagators())
			if err != nil {
				return nil, err
			}

			// As a special case, we handle __temporal_workflow_metadata query
			// here instead of in workflowExecutionEventHandlerImpl.ProcessQuery
			// because we need the context environment to do so.
			// __temporal_workflow_metadata 是 SDK 内置 query。
			// 它需要访问 workflow Context 里的 handler/metadata 信息，所以在这里直接处理。
			if queryType == QueryTypeWorkflowMetadata {
				if result, err := getWorkflowMetadata(rootCtx); err != nil {
					return nil, err
				} else {
					// Use raw value built from default converter because we don't want to use
					// user-conversion
					// 先用默认 DataConverter 把 metadata 转成 payload，
					// 再包装成 RawValue 交给 workflow 当前 DataConverter 编码；
					// 注释说明这样做是为了避免 metadata 本身被用户 converter 改写。
					resultPayload, err := converter.GetDefaultDataConverter().ToPayload(result)
					if err != nil {
						return nil, err
					}
					return encodeArg(getDataConverterFromWorkflowContext(rootCtx), converter.NewRawValue(resultPayload))
				}
			}

			// 从 workflow Context 中取 workflowEnvOptions。
			// 用户调用 workflow.SetQueryHandler 时，会把 query handler 注册到 eo.queryHandlers。
			eo := getWorkflowEnvOptions(rootCtx)
			// A handler must be present since it is needed for argument decoding,
			// even if the interceptor intercepts query handling
			// 即使 interceptor 最终要拦截 query，SDK 也必须先找到 handler，
			// 因为参数解码需要 handler.fn 的函数签名和 handler.dataConverter。
			handler, ok := eo.queryHandlers[queryType]
			if !ok {
				// 未注册 query handler 时，返回已知 query 类型列表。
				// 这里包含三个内置 query，再追加用户注册的 queryHandlers key。
				keys := []string{QueryTypeStackTrace, QueryTypeOpenSessions, QueryTypeWorkflowMetadata}
				for k := range eo.queryHandlers {
					keys = append(keys, k)
				}
				return nil, fmt.Errorf("unknown queryType %v. KnownQueryTypes=%v", queryType, keys)
			}

			// Decode the arguments
			// 根据用户 query handler 的函数签名，把 queryArgs payload 解码成 Go 参数。
			args, err := decodeArgsToRawValues(handler.dataConverter, reflect.TypeOf(handler.fn), queryArgs)
			if err != nil {
				return nil, fmt.Errorf("unable to decode the input for queryType: %v, with error: %w", handler.queryType, err)
			}

			// Invoke
			// 通过 inbound interceptor 调用 query。
			// 默认 workflowEnvironmentInterceptor.HandleQuery 会找到 eo.queryHandlers[in.QueryType]，
			// 然后执行 handler.execute(in.Args)。
			result, err := envInterceptor.inboundInterceptor.HandleQuery(
				rootCtx,
				&HandleQueryInput{QueryType: queryType, Args: args},
			)

			// Encode the result
			// query handler 成功后，用 handler.dataConverter 把返回值编码成 Payloads。
			var serializedResult *commonpb.Payloads
			if err == nil {
				serializedResult, err = encodeArg(handler.dataConverter, result)
			}
			return serializedResult, err
		},
	)
}

func (d *syncWorkflowDefinition) OnWorkflowTaskStarted(deadlockDetectionTimeout time.Duration) {
	executeDispatcher(d.rootCtx, d.dispatcher, deadlockDetectionTimeout)
}

func (d *syncWorkflowDefinition) StackTrace() string {
	return d.dispatcher.StackTrace()
}

func (d *syncWorkflowDefinition) Close() {
	if d.dispatcher != nil {
		d.dispatcher.Close()
	}
}

// NewDispatcher creates a new Dispatcher instance with a root coroutine function.
// Context passed to the root function is child of the passed rootCtx.
// This way rootCtx can be used to pass values to the coroutine code.
func newDispatcher(rootCtx Context, interceptor *workflowEnvironmentInterceptor, root func(ctx Context), allBlockedCallback func() bool) (*dispatcherImpl, Context) {
	env := getWorkflowEnvironment(rootCtx)

	result := &dispatcherImpl{
		interceptor:        interceptor.outboundInterceptor,
		logger:             env.GetLogger(),
		deadlockDetector:   newDeadlockDetector(),
		allBlockedCallback: allBlockedCallback,
	}
	interceptor.dispatcher = result
	ctxWithState := result.interceptor.Go(rootCtx, "root", root)
	return result, ctxWithState
}

// executeDispatcher executed coroutines in the calling thread and calls workflow completion callbacks
// if root workflow function returned
func executeDispatcher(ctx Context, dispatcher dispatcher, timeout time.Duration) {
	// 从 workflow Context 中取出 WorkflowEnvironment。
	// 这里的 env 实际上通常是 workflowExecutionEventHandlerImpl / workflowEnvironmentImpl，
	// env.Complete 会把 workflow 的最终 result/error 写回 SDK 内部状态，后续 CompleteWorkflowTask
	// 会据此生成 CompleteWorkflowExecution / FailWorkflowExecution / ContinueAsNew 等 command。
	env := getWorkflowEnvironment(ctx)
	// 让 dispatcher 运行所有 ready 的 workflow coroutines。
	// ExecuteUntilAllBlocked 会按确定性顺序逐个唤醒 coroutine，
	// 一直跑到所有 coroutine 都阻塞、全部结束、或检测到 panic/deadlock。
	// 这里不会直接调用 server；它只推进本地 workflow runtime。
	panicErr := dispatcher.ExecuteUntilAllBlocked(timeout)
	if panicErr != nil {
		// 如果某个 coroutine panic 或 deadlock detector 产生 workflowPanicError，
		// dispatcher 会把错误返回到这里。
		// env.Complete(nil, panicErr) 表示 workflow 以这个错误完成到 SDK 内部状态，
		// 后续 workflow task completion 逻辑会按 panic policy 生成失败或 WFT failed。
		env.Complete(nil, panicErr)
		return
	}

	// root workflow coroutine 返回时，会在 syncWorkflowDefinition.Execute 创建的 root func 里
	// 把 *workflowResult 写进 workflowResultContextKey 保存的 **workflowResult。
	// 这里解引用后如果仍然是 nil，说明用户 workflow 函数还没 return，
	// 只是当前能跑的 coroutine 都阻塞了，例如阻塞在 Activity Future.Get、workflow.Sleep、signal Receive 等。
	rp := *getWorkflowResultPointerPointer(ctx)
	if rp == nil {
		// Result is not set, so workflow is still executing
		// workflow 还没结束，本次 WFT 可能会带出 commands，也可能只是等待 local activity/update 等。
		// 这里直接返回，不调用 env.Complete。
		return
	}

	// 走到这里说明 root workflow 函数已经 return 了，rp 里有 workflowResult 和 error。
	// 在真正 Complete 之前，SDK 做一些收尾检查和日志提示。
	weo := getWorkflowEnvOptions(ctx)
	// 检查 signal channel 里是否还有未消费的 signal。
	// getUnhandledSignalNames 会尝试从每个 signal channel 取一个值；
	// 如果取到了，会把值放回 ch.recValue，避免检查动作真的消费掉 signal。
	us := weo.getUnhandledSignalNames()
	if len(us) > 0 {
		// workflow 已经 return，但还有 signal 没被用户代码接收，记录 warning。
		env.GetLogger().Warn("Workflow has unhandled signals", "SignalNames", us)
	}
	// Warn if there are any update handlers still running
	// warnUpdate 是日志里输出的 update 信息，只保留 name 和 id。
	type warnUpdate struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	}
	// 收集 workflow return 时仍在运行、且 unfinished policy 是 WarnAndAbandon 的 update handler。
	var updatesToWarn []warnUpdate
	for _, info := range weo.getRunningUpdateHandles() {
		// runningUpdatesHandles 只记录还没结束的 update；
		// updateHandlers[info.Name].unfinishedPolicy 决定 workflow 结束时遇到未完成 update 如何处理。
		if weo.updateHandlers[info.Name].unfinishedPolicy == HandlerUnfinishedPolicyWarnAndAbandon {
			updatesToWarn = append(updatesToWarn, warnUpdate{
				Name: info.Name,
				ID:   info.ID,
			})
		}
	}

	// Verify that the workflow did not fail. If it did we will not warn about unhandled updates.
	// workflow 如果是正常完成、CanceledError、ContinueAsNewError，则对未完成 update 记 warning。
	// 如果 workflow 自己失败了，则不再额外提示未完成 update，避免用次要 warning 干扰真正失败原因。
	var canceledErr *CanceledError
	var contErr *ContinueAsNewError
	if len(updatesToWarn) > 0 && (rp.error == nil || errors.As(rp.error, &canceledErr) || errors.As(rp.error, &contErr)) {
		env.GetLogger().Warn(unhandledUpdateWarningMessage, "Updates", updatesToWarn)
	}

	// 把 root workflow 函数的返回值/错误交给 WorkflowEnvironment。
	// 这一步只更新 SDK 内部完成状态；真正发给 server 要等外层 ProcessWorkflowTask
	// 调 CompleteWorkflowTask/RespondWorkflowTaskCompleted。
	env.Complete(rp.workflowResult, rp.error)
}

// For troubleshooting stack pretty printing only.
// Set to true to see full stack trace that includes framework methods.
const disableCleanStackTraces = false

func getState(ctx Context) *coroutineState {
	s := ctx.Value(coroutinesContextKey)
	if s == nil {
		panic("getState: not workflow context")
	}
	state := s.(*coroutineState)
	if !state.dispatcher.IsExecuting() {
		panic(panicIllegalAccessCoroutineState)
	}
	return state
}

func assertNotInReadOnlyState(ctx Context) {
	state := getState(ctx)
	// use the dispatcher state instead of the coroutine state because contexts can be
	// shared
	if state.dispatcher.getIsReadOnly() {
		panic(panicIllegalAccessCoroutineState)
	}
}

func assertNotInReadOnlyStateCancellation(ctx Context) {
	s := ctx.Value(coroutinesContextKey)
	if s == nil {
		panic("assertNotInReadOnlyStateCtxCancellation: not workflow context")
	}
	state := s.(*coroutineState)
	// For cancellation the dispatcher may not be running because workflow cancellation
	// is sent outside of the dispatchers loop.
	if state.dispatcher.IsClosed() {
		panic(panicIllegalAccessCoroutineState)
	}
	// use the dispatcher state instead of the coroutine state because contexts can be
	// shared
	if state.dispatcher.getIsReadOnly() {
		panic(panicIllegalAccessCoroutineState)
	}
}

func getStateIfRunning(ctx Context) *coroutineState {
	if ctx == nil {
		return nil
	}
	s := ctx.Value(coroutinesContextKey)
	if s == nil {
		return nil
	}
	state := s.(*coroutineState)
	if !state.dispatcher.IsExecuting() {
		return nil
	}
	return state
}

func (c *channelImpl) Name() string {
	return c.name
}

func (c *channelImpl) CanReceiveWithoutBlocking() bool {
	return c.recValue != nil || len(c.buffer) > 0 || len(c.blockedSends) > 0 || c.closed
}

func (c *channelImpl) CanSendWithoutBlocking() bool {
	return len(c.buffer) < c.size || len(c.blockedReceives) > 0
}

func (c *channelImpl) Receive(ctx Context, valuePtr interface{}) (more bool) {
	assertNotInReadOnlyState(ctx)
	state := getState(ctx)
	hasResult := false
	var result interface{}
	callback := &receiveCallback{
		fn: func(v interface{}, m bool) bool {
			result = v
			hasResult = true
			more = m
			return true
		},
	}

	for {
		hasResult = false
		v, ok, m := c.receiveAsyncImpl(callback)

		if !ok && !m { // channel closed and empty
			return m
		}

		if ok || !m {
			err := c.assignValue(v, valuePtr)
			if err == nil {
				state.unblocked()
				return m
			}
			continue // corrupt signal. Drop and reset process
		}
		for {
			if hasResult {
				err := c.assignValue(result, valuePtr)
				if err == nil {
					state.unblocked()
					return more
				}
				break // Corrupt signal. Drop and reset process.
			}
			state.yield("blocked on " + c.name + ".Receive")
		}
	}

}

func (c *channelImpl) ReceiveWithTimeout(ctx Context, timeout time.Duration, valuePtr interface{}) (ok, more bool) {
	okAwait, err := AwaitWithTimeout(ctx, timeout, func() bool { return c.Len() > 0 })
	if err != nil { // context canceled
		return false, true
	}
	if !okAwait { // timed out
		return false, true
	}
	ok, more = c.ReceiveAsyncWithMoreFlag(valuePtr)
	if !ok {
		panic("unexpected empty channel")
	}
	return true, more
}

func (c *channelImpl) ReceiveAsync(valuePtr interface{}) (ok bool) {
	ok, _ = c.ReceiveAsyncWithMoreFlag(valuePtr)
	return ok
}

func (c *channelImpl) ReceiveAsyncWithMoreFlag(valuePtr interface{}) (ok bool, more bool) {
	for {
		v, ok, more := c.receiveAsyncImpl(nil)
		if !ok && !more { // channel closed and empty
			return ok, more
		}

		err := c.assignValue(v, valuePtr)
		if err != nil {
			continue
			// keep consuming until a good signal is hit or channel is drained
		}
		return ok, more
	}
}

func (c *channelImpl) Len() int {
	result := len(c.buffer) + len(c.blockedSends)
	if c.recValue != nil {
		result = result + 1
	}
	return result
}

// ok = true means that value was received
// more = true means that channel is not closed and more deliveries are possible
func (c *channelImpl) receiveAsyncImpl(callback *receiveCallback) (v interface{}, ok bool, more bool) {
	if c.recValue != nil {
		r := *c.recValue
		c.recValue = nil
		return r, true, true
	}
	if len(c.buffer) > 0 {
		r := c.buffer[0]
		c.buffer[0] = nil
		c.buffer = c.buffer[1:]

		// Move blocked sends into buffer
		for len(c.blockedSends) > 0 {
			b := c.blockedSends[0]
			c.blockedSends[0] = nil
			c.blockedSends = c.blockedSends[1:]
			if b.fn() {
				c.buffer = append(c.buffer, b.value)
				break
			}
		}

		return r, true, true
	}
	if c.closed {
		return nil, false, false
	}
	for len(c.blockedSends) > 0 {
		b := c.blockedSends[0]
		c.blockedSends[0] = nil
		c.blockedSends = c.blockedSends[1:]
		if b.fn() {
			return b.value, true, true
		}
	}
	if callback != nil {
		c.blockedReceives = append(c.blockedReceives, callback)
	}
	return nil, false, true
}

func (c *channelImpl) removeReceiveCallback(callback *receiveCallback) {
	for i, blockedCallback := range c.blockedReceives {
		if callback == blockedCallback {
			c.blockedReceives = append(c.blockedReceives[:i], c.blockedReceives[i+1:]...)
			break
		}
	}
}

func (c *channelImpl) removeSendCallback(callback *sendCallback) {
	for i, blockedCallback := range c.blockedSends {
		if callback == blockedCallback {
			c.blockedSends = append(c.blockedSends[:i], c.blockedSends[i+1:]...)
			break
		}
	}
}

func (c *channelImpl) Send(ctx Context, v interface{}) {
	state := getState(ctx)
	valueConsumed := false
	callback := &sendCallback{
		value: v,
		fn: func() bool {
			valueConsumed = true
			return true
		},
	}
	ok := c.sendAsyncImpl(v, callback)
	if ok {
		state.unblocked()
		return
	}
	for {
		if valueConsumed {
			state.unblocked()
			return
		}

		// Check for closed in the loop as close can be called when send is blocked
		if c.closed {
			panic("Closed channel")
		}
		state.yield("blocked on " + c.name + ".Send")
	}
}

func (c *channelImpl) SendAsync(v interface{}) (ok bool) {
	return c.sendAsyncImpl(v, nil)
}

func (c *channelImpl) sendAsyncImpl(v interface{}, pair *sendCallback) (ok bool) {
	if c.closed {
		panic("Closed channel")
	}
	for len(c.blockedReceives) > 0 {
		blockedGet := c.blockedReceives[0].fn
		c.blockedReceives[0] = nil
		c.blockedReceives = c.blockedReceives[1:]
		// false from callback indicates that value wasn't consumed
		if blockedGet(v, true) {
			return true
		}
	}
	if len(c.buffer) < c.size {
		c.buffer = append(c.buffer, v)
		return true
	}
	if pair != nil {
		c.blockedSends = append(c.blockedSends, pair)
	}
	return false
}

func (c *channelImpl) Close() {
	c.closed = true
	// Use a copy of blockedReceives for iteration as invoking callback could result in modification
	copy := append(c.blockedReceives[:0:0], c.blockedReceives...)
	for _, callback := range copy {
		callback.fn(nil, false)
	}
	// All blocked sends are going to panic
}

// Takes a value and assigns that 'to' value. logs a metric if it is unable to deserialize
func (c *channelImpl) assignValue(from interface{}, to interface{}) error {
	err := decodeAndAssignValue(c.dataConverter, from, to)
	// add to metrics
	if err != nil {
		c.env.GetLogger().Error(fmt.Sprintf("Deserialization error. Corrupted signal received on channel %s.", c.name), tagError, err)
		c.env.GetMetricsHandler().Counter(metrics.CorruptedSignalsCounter).Inc(1)
	}
	return err
}

// initialYield is called at the beginning of coroutine execution.
// stackDepth is the depth of the top of the stack to omit when a stack trace is generated,
// to hide frames internal to the framework.
func (s *coroutineState) initialYield(stackDepth int, status string) {
	if s.blocked.Swap(true) {
		panic("trying to block on coroutine which is already blocked, most likely a wrong Context is used to do blocking" +
			" call (like Future.Get() or Channel.Receive()")
	}
	keepBlocked := true
	for keepBlocked {
		f := <-s.unblock
		keepBlocked = f(status, stackDepth+1)
	}
	s.blocked.Swap(false)
}

// isPanicking reports whether the current goroutine is executing during panic unwinding. It checks
// for runtime.gopanic on the call stack via runtime.Callers().
func isPanicking() bool {
	var pcs [20]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if frame.Function == "runtime.gopanic" {
			return true
		}
		if !more {
			break
		}
	}
	return false
}

// yield indicates that coroutine cannot make progress and should sleep
// this call blocks
func (s *coroutineState) yield(status string) {
	if isPanicking() {
		// Unfortunately we lose the real panic message here, but the stack trace will still contain
		// the right lines.
		panic(errors.New(
			"yield during panic unwinding: a deferred function attempted to block " +
				"the coroutine while a panic was in progress"))
	}
	s.aboutToBlock <- true
	s.initialYield(3, status) // omit three levels of stack. To adjust change to 0 and count the lines to remove.
	s.keptBlocked = true
}

func getStackTrace(coroutineName, status string, stackDepth int) string {
	top := fmt.Sprintf("coroutine %s [%s]:", coroutineName, status)
	// Omit top stackDepth frames + top status line.
	// Omit bottom two frames which is wrapping of coroutine in a goroutine.
	return getStackTraceRaw(top, stackDepth*2+1, 4)
}

func getStackTraceRaw(top string, omitTop, omitBottom int) string {
	stack := stackBuf[:runtime.Stack(stackBuf[:], false)]
	outStack := filterStackTrace(string(stack), omitTop, omitBottom)
	return strings.Join([]string{top, outStack}, "\n")
}

func filterStackTrace(stack string, omitTop, omitBottom int) string {
	stack = strings.TrimRightFunc(stack, unicode.IsSpace)
	if disableCleanStackTraces {
		return stack
	}

	lines := strings.Split(stack, "\n")
	omitEnd := len(lines) - omitBottom
	// If the start is after the end, the depth was invalid originally so return
	// the entire raw stack
	if omitTop > omitEnd {
		return stack
	}
	return strings.Join(lines[omitTop:omitEnd], "\n")
}

func getCoroStackTrace(crt *coroutineState, status string, stackDepth int) (string, error) {
	// Can't dump goroutines selectively :(
	// Instead, we identify a coroutine's stack trace by the *coroutineState pointer address
	// in its function arguments. To avoid false positives, we also match on the fixed
	// member function name.
	stacks := stackBuf[:runtime.Stack(stackBuf[:], true)]
	needle := []byte(fmt.Sprintf("/internal.(*coroutineState).run(%p,", crt))
	idx := bytes.Index(stacks, needle)
	if idx == -1 {
		if len(stacks) == len(stackBuf) {
			return "", fmt.Errorf("coroutine not found: %w", errStackTraceTruncated)
		}
		// NOTE: This could happen if coroutineState is moved between runtime.Stack(...)
		// and formatting needle. However, Go's GC is currently non-moving.
		return "", errCoroStackNotFound
	}

	// coroStack spans from the stackDelim before idx to the stackDelim after idx
	stackDelim := []byte("\n\n")
	coroStack := stacks
	if start := bytes.LastIndex(stacks[:idx], stackDelim); start != -1 {
		start += len(stackDelim) // skip over delimiter
		coroStack = stacks[start:]
	}
	coroStack, _, _ = bytes.Cut(coroStack, stackDelim)

	// Omit top stackDepth frames + top status line.
	// Omit bottom two frames which is wrapping of coroutine in a goroutine.
	outStack := filterStackTrace(string(coroStack), stackDepth*2+1, 4)
	return fmt.Sprintf("coroutine %s [%s]:\n%s", crt.name, status, outStack), nil
}

// unblocked is called by coroutine to indicate that since the last time yield was unblocked channel or select
// where unblocked versus calling yield again after checking their condition
func (s *coroutineState) unblocked() {
	s.keptBlocked = false
}

func (s *coroutineState) call(timeout time.Duration) {
	s.unblock <- func(status string, stackDepth int) bool {
		return false // unblock
	}

	// Defaults are populated in the worker options during worker startup, but test environment
	// may have no default value for the deadlock detection timeout, so we also need to set it here for
	// backwards compatibility.
	if timeout == 0 {
		timeout = defaultDeadlockDetectionTimeout
		if debugMode {
			timeout = unlimitedDeadlockDetectionTimeout
		}
	}
	deadlockTicker := s.dispatcher.deadlockDetector.begin(timeout)
	defer deadlockTicker.end()

	select {
	case <-s.aboutToBlock:
	case <-deadlockTicker.reached():
		// Use workflowPanicError since this used to call panic(msg)
		st, err := getCoroStackTrace(s, "running", 0)
		if err != nil {
			st = fmt.Sprintf("<%s>", err)
		}
		msg := fmt.Sprintf("[TMPRL1101] Potential deadlock detected: "+
			"workflow goroutine %q didn't yield for over a second", s.name)
		s.closed.Store(true)
		s.panicError = newWorkflowPanicError(msg, st)
	}
}

func (s *coroutineState) close() {
	s.closed.Store(true)
	s.aboutToBlock <- true
}

// exit tries to run Goexit on the coroutine and wait for it to exit
// within timeout. If it doesn't exit within timeout, it will log a warning.
func (s *coroutineState) exit(logger log.Logger, warnTimeout time.Duration) {
	if !s.closed.Load() {
		s.unblock <- func(status string, stackDepth int) bool {
			runtime.Goexit()
			return true
		}

		timer := time.NewTimer(warnTimeout)
		defer timer.Stop()

		select {
		case <-s.aboutToBlock:
			return
		case <-timer.C:
			st, err := getCoroStackTrace(s, "running", 0)
			if err != nil {
				st = fmt.Sprintf("<%s>", err)
			}

			logger.Warn(fmt.Sprintf("Workflow coroutine %q didn't exit within %v", s.name, warnTimeout), "stackTrace", st)
		}
		// We need to make sure the coroutine is closed, otherwise we risk concurrent coroutines running
		// at the same time causing a race condition.
		<-s.aboutToBlock
	}
}

func (s *coroutineState) stackTrace() string {
	if s.closed.Load() {
		return ""
	}
	stackCh := make(chan string, 1)
	s.unblock <- func(status string, stackDepth int) bool {
		stackCh <- getStackTrace(s.name, status, stackDepth+2)
		return true
	}
	return <-stackCh
}

func (s *coroutineState) run(ctx Context, f func(ctx Context)) {
	defer runtime.KeepAlive(&s) // keep receiver argument alive for getCoroStackTrace
	defer s.close()
	defer func() {
		if r := recover(); r != nil {
			st := getStackTrace(s.name, "panic", 4)
			s.panicError = newWorkflowPanicError(r, st)
		}
	}()
	s.initialYield(1, "")
	f(ctx)
}

func (d *dispatcherImpl) NewCoroutine(ctx Context, name string, highPriority bool, f func(ctx Context)) Context {
	if name == "" {
		name = fmt.Sprintf("%v", d.sequence+1)
	}
	state := d.newState(name, highPriority)
	spawned := WithValue(ctx, coroutinesContextKey, state)
	go state.run(spawned, f)
	return spawned
}

func (d *dispatcherImpl) newState(name string, highPriority bool) *coroutineState {
	c := &coroutineState{
		name:         name,
		dispatcher:   d,
		aboutToBlock: make(chan bool, 1),
		unblock:      make(chan unblockFunc),
	}
	d.sequence++
	if highPriority {
		// Update requests need to be added to the front of the dispatchers coroutine list so they
		// are handled before the root coroutine.
		d.newEagerCoroutines = append(d.newEagerCoroutines, c)
	} else {
		d.coroutines = append(d.coroutines, c)
	}
	return c
}

func (d *dispatcherImpl) IsClosed() bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.closed
}

func (d *dispatcherImpl) ExecuteUntilAllBlocked(deadlockDetectionTimeout time.Duration) (err error) {
	// dispatcher 的执行状态由 mutex 保护。
	// 这里先检查 dispatcher 是否已经关闭；关闭后不能再调度 coroutine。
	d.mutex.Lock()
	if d.closed {
		d.mutex.Unlock()
		panic("dispatcher is closed")
	}
	// 防止重入。
	// ExecuteUntilAllBlocked 只能由外层 workflow task 处理线程调用；
	// 如果 workflow coroutine 自己又递归触发 dispatcher 执行，会破坏确定性调度顺序。
	if d.executing {
		d.mutex.Unlock()
		panic("call to ExecuteUntilAllBlocked (possibly from a coroutine) while it is already running")
	}
	// 标记 dispatcher 正在执行。
	// getState(ctx) 会检查 dispatcher.IsExecuting()，确保阻塞类 workflow API 只能在 dispatcher 正在调度时使用。
	d.executing = true
	d.mutex.Unlock()
	// 无论本函数正常返回还是因为 panic unwind，最终都要清除 executing 标记。
	defer func() {
		d.mutex.Lock()
		d.executing = false
		d.mutex.Unlock()
	}()
	// allBlocked 表示当前一轮扫描后，所有 coroutine 都没有取得新进展，只是继续阻塞。
	// 初始设为 false，是为了至少进入一次循环，让 coroutine 有机会从 initialYield 中被唤醒。
	allBlocked := false
	// Keep executing until at least one goroutine made some progress
	// 循环条件含义：
	// - !allBlocked：上一轮至少有 coroutine 前进/结束/创建新 coroutine，因此还要继续扫描；
	// - d.allBlockedCallback()：即使所有 coroutine 都阻塞，也给环境一次机会处理积压工作。
	//   对 workflow 来说，这个 callback 是 DrainUnhandledUpdates，可能把没有 handler 的 buffered update
	//   调度出来并拒绝；如果它返回 true，说明状态被更新了，需要再跑一轮。
	for !allBlocked || d.allBlockedCallback() {
		// highPriority/eager coroutine 会先放到 newEagerCoroutines。
		// 每轮开始时把它们插到普通 coroutine 队列最前面，保证例如 Update handler 这类高优先级任务先跑。
		d.coroutines = append(d.newEagerCoroutines, d.coroutines...)
		d.newEagerCoroutines = nil
		// Give every coroutine chance to execute removing closed ones
		// 乐观假设本轮所有 coroutine 都会保持阻塞；
		// 后面只要发现某个 coroutine 结束、前进、或创建新 coroutine，就把 allBlocked 改回 false。
		allBlocked = true
		// 记录本轮开始前的 coroutine sequence。
		// NewCoroutine 会递增 d.sequence；循环末尾用它判断本轮是否创建了新 coroutine。
		lastSequence := d.sequence
		// 按 d.coroutines 顺序给每个 coroutine 一次执行机会。
		// 这个顺序就是 SDK workflow coroutine 的确定性调度顺序。
		for i := 0; i < len(d.coroutines); i++ {
			c := d.coroutines[i]
			if !c.closed.Load() {
				// TODO: Support handling of panic in a coroutine by dispatcher.
				// TODO: Dump all outstanding coroutines if one of them panics
				// c.call 会向 coroutine 的 unblock channel 发送一个 unblock 函数，
				// 让 coroutine 从 yield/initialYield 处继续执行；
				// 然后 c.call 等待 coroutine 再次 aboutToBlock、结束，或 deadlock timeout。
				c.call(deadlockDetectionTimeout)
			}
			// c.call() can close the context so check again
			if c.closed.Load() {
				// remove the closed one from the slice
				// coroutine 已结束，从 dispatcher 队列里移除。
				// i-- 是为了让下一轮 for 继续检查移动到当前位置的元素。
				d.coroutines = append(d.coroutines[:i],
					d.coroutines[i+1:]...)
				i--
				if c.panicError != nil {
					// coroutine.run 捕获到 panic，或 c.call 检测到 deadlock 后，
					// 会把错误放到 c.panicError。
					// 这里直接返回给 executeDispatcher，由它调用 env.Complete(nil, panicErr)。
					return c.panicError
				}
				// 有 coroutine 结束，说明本轮状态发生变化；
				// 不能认为所有 coroutine 都保持阻塞，需要继续外层循环。
				allBlocked = false

			} else {
				// coroutine 没结束时，用 keptBlocked 判断它是否只是“被唤醒后仍然阻塞”。
				// yield 会设置 keptBlocked=true；如果 coroutine 取得进展，会调用 unblocked() 把它设为 false。
				// 因此只要有一个未关闭 coroutine 的 keptBlocked=false，allBlocked 就会变成 false。
				allBlocked = allBlocked && (c.keptBlocked || c.closed.Load())
			}
			// If any eager coroutines were created by the last coroutine we
			// need to schedule them now.
			// 当前 coroutine 执行期间可能创建 highPriority coroutine，例如 Update handler。
			// 这些 eager coroutine 要插到当前 i 后面，尽快在本轮继续执行，而不是等下一轮。
			if len(d.newEagerCoroutines) > 0 {
				d.coroutines = slices.Insert(d.coroutines, i+1, d.newEagerCoroutines...)
				d.newEagerCoroutines = nil
				// 创建了新的 coroutine，说明本轮状态有新工作，外层循环不能结束。
				allBlocked = false
			}
		}
		// Set allBlocked to false if new coroutines where created
		// 如果本轮开始后 d.sequence 变化，说明创建了普通或 eager coroutine。
		// 即使上面的判断没有捕捉到，也要把 allBlocked 置为 false，保证新 coroutine 有机会运行。
		allBlocked = allBlocked && lastSequence == d.sequence
	}
	// 走到这里表示：
	// - 所有 coroutine 都阻塞或已经结束；
	// - allBlockedCallback 也没有产生新工作；
	// - 没有 panic/deadlock。
	// 对外层 executeDispatcher 来说，这意味着本次本地 workflow runtime 推进完成。
	return nil
}

func (d *dispatcherImpl) IsDone() bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return len(d.coroutines) == 0
}

func (d *dispatcherImpl) IsExecuting() bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.executing
}

func (d *dispatcherImpl) getIsReadOnly() bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.readOnly
}

func (d *dispatcherImpl) setIsReadOnly(readOnly bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.readOnly = readOnly
}

func (d *dispatcherImpl) Close() {
	d.mutex.Lock()
	if d.closed {
		d.mutex.Unlock()
		return
	}
	d.closed = true
	d.mutex.Unlock()
	// We need to exit the coroutines in a separate goroutine because:
	// 	* The coroutine may be stuck and won't respond to the exit request.
	// 	* On exit the coroutines defers will still run and that may block.
	go func() {
		for _, c := range d.coroutines {
			c.exit(d.logger, defaultDeadlockDetectionTimeout)
		}
	}()
}

func (d *dispatcherImpl) StackTrace() string {
	var result string
	for i := 0; i < len(d.coroutines); i++ {
		c := d.coroutines[i]
		if !c.closed.Load() {
			if len(result) > 0 {
				result += "\n\n"
			}
			result += c.stackTrace()
		}
	}
	return result
}

func (s *selectorImpl) AddReceive(c ReceiveChannel, f func(c ReceiveChannel, more bool)) Selector {
	s.cases = append(s.cases, &selectCase{channel: c.(*channelImpl), receiveFunc: &f})
	return s
}

func (s *selectorImpl) AddSend(c SendChannel, v interface{}, f func()) Selector {
	s.cases = append(s.cases, &selectCase{channel: c.(*channelImpl), sendFunc: &f, sendValue: &v})
	return s
}

func (s *selectorImpl) AddFuture(future Future, f func(future Future)) Selector {
	asyncF, ok := future.(asyncFuture)
	if !ok {
		panic("cannot chain Future that wasn't created with workflow.NewFuture")
	}
	s.cases = append(s.cases, &selectCase{future: asyncF, futureFunc: &f})
	return s
}

func (s *selectorImpl) AddDefault(f func()) {
	s.defaultFunc = &f
}

func (s *selectorImpl) HasPending() bool {
	for _, pair := range s.cases {
		if pair.receiveFunc != nil && pair.channel.CanReceiveWithoutBlocking() {
			return true
		} else if pair.sendFunc != nil && pair.channel.CanSendWithoutBlocking() {
			return true
		} else if pair.futureFunc != nil && pair.future.IsReady() {
			return true
		}
	}
	return false
}

func (s *selectorImpl) Select(ctx Context) {
	assertNotInReadOnlyState(ctx)
	state := getState(ctx)
	var readyBranch func()
	var cleanups []func()
	defer func() {
		for _, c := range cleanups {
			c()
		}
	}()

	for _, pair := range s.cases {
		if pair.receiveFunc != nil {
			f := *pair.receiveFunc
			c := pair.channel
			hasDefault := s.defaultFunc != nil
			callback := &receiveCallback{
				fn: func(v interface{}, more bool) bool {
					if readyBranch != nil {
						return false
					}
					env := getWorkflowEnvironment(ctx)
					dropSignalFlag := env.TryUse(SDKFlagBlockedSelectorSignalReceive)
					channelLostMsgFlag := env.TryUse(SDKFlagWorkflowNewChannelLostMessages)

					// Pre-store c.recValue to prevent signal loss when AddDefault
					// blocks. Without channelLostMsgFlag, always pre-store (original
					// #1624 fix). With channelLostMsgFlag, only pre-store when a
					// default branch exists to avoid overwriting c.recValue when
					// multiple selectors are blocked on the same channel.
					storeNow := dropSignalFlag && (!channelLostMsgFlag || hasDefault)
					if storeNow {
						c.recValue = &v
					}

					readyBranch = func() {
						if !storeNow {
							c.recValue = &v
						}
						f(c, more)
					}
					return true
				},
			}
			v, ok, more := c.receiveAsyncImpl(callback)
			if ok || !more {
				// Select() returns in this case/branch. The callback won't be called for this case. However, callback
				// will be called for previous cases/branches. We should set readyBranch so that when other case/branch
				// become ready they won't consume the value for this Select() call.
				readyBranch = func() {
				}
				// Avoid assigning pointer to nil interface which makes
				// c.RecValue != nil and breaks the nil check at the beginning of receiveAsyncImpl
				if more {
					c.recValue = &v
				} else {
					pair.receiveFunc = nil
				}
				f(c, more)
				return
			}
			// callback closure is added to channel's blockedReceives, we need to clean it up to avoid closure leak
			cleanups = append(cleanups, func() {
				c.removeReceiveCallback(callback)
			})
		} else if pair.sendFunc != nil {
			f := *pair.sendFunc
			c := pair.channel
			callback := &sendCallback{
				value: *pair.sendValue,
				fn: func() bool {
					if readyBranch != nil {
						return false
					}
					readyBranch = func() {
						f()
					}
					return true
				},
			}
			ok := c.sendAsyncImpl(*pair.sendValue, callback)
			if ok {
				// Select() returns in this case/branch. The callback won't be called for this case. However, callback
				// will be called for previous cases/branches. We should set readyBranch so that when other case/branch
				// become ready they won't consume the value for this Select() call.
				readyBranch = func() {
				}
				f()
				return
			}
			// callback closure is added to channel's blockedSends, we need to clean it up to avoid closure leak
			cleanups = append(cleanups, func() {
				c.removeSendCallback(callback)
			})
		} else if pair.futureFunc != nil {
			p := pair
			f := *p.futureFunc
			callback := &receiveCallback{
				fn: func(v interface{}, more bool) bool {
					if readyBranch != nil {
						return false
					}
					readyBranch = func() {
						p.futureFunc = nil
						f(p.future)
					}
					return true
				},
			}

			_, ok, _ := p.future.GetAsync(callback)
			if ok {
				// Select() returns in this case/branch. The callback won't be called for this case. However, callback
				// will be called for previous cases/branches. We should set readyBranch so that when other case/branch
				// become ready they won't consume the value for this Select() call.
				readyBranch = func() {
				}
				p.futureFunc = nil
				f(p.future)
				return
			}
			// callback closure is added to future's channel's blockedReceives, need to clean up to avoid leak
			cleanups = append(cleanups, func() {
				p.future.RemoveReceiveCallback(callback)
			})
		}
	}
	if s.defaultFunc != nil {
		f := *s.defaultFunc
		f()
		return
	}
	for {
		if readyBranch != nil {
			readyBranch()
			state.unblocked()
			return
		}
		state.yield("blocked on " + s.name + ".Select")
	}
}

// NewWorkflowDefinition creates a WorkflowDefinition from a Workflow
func newSyncWorkflowDefinition(workflow workflow) *syncWorkflowDefinition {
	return &syncWorkflowDefinition{workflow: workflow}
}

func getValidatedWorkflowFunction(workflowFunc interface{}, args []interface{}, dataConverter converter.DataConverter, r *registry) (*WorkflowType, *commonpb.Payloads, error) {
	if err := validateFunctionArgs(workflowFunc, args, true); err != nil {
		return nil, nil, err
	}

	fnName, err := getWorkflowFunctionName(r, workflowFunc)
	if err != nil {
		return nil, nil, err
	}

	if dataConverter == nil {
		dataConverter = converter.GetDefaultDataConverter()
	}
	input, err := encodeArgs(dataConverter, args)
	if err != nil {
		return nil, nil, err
	}
	return &WorkflowType{Name: fnName}, input, nil
}

func getWorkflowEnvOptions(ctx Context) *WorkflowOptions {
	options := ctx.Value(workflowEnvOptionsContextKey)
	if options != nil {
		return options.(*WorkflowOptions)
	}
	return nil
}

func setWorkflowEnvOptionsIfNotExist(ctx Context) Context {
	options := getWorkflowEnvOptions(ctx)
	var newOptions WorkflowOptions
	if options != nil {
		newOptions = *options
	} else {
		newOptions.signalChannels = make(map[string]Channel)
		newOptions.requestedSignalChannels = make(map[string]*requestedSignalChannel)
		newOptions.queryHandlers = make(map[string]*queryHandler)
		newOptions.updateHandlers = make(map[string]*updateHandler)
		newOptions.runningUpdatesHandles = make(map[string]UpdateInfo)
	}
	if newOptions.DataConverter == nil {
		newOptions.DataConverter = converter.GetDefaultDataConverter()
	}

	return WithValue(ctx, workflowEnvOptionsContextKey, &newOptions)
}

func getDataConverterFromWorkflowContext(ctx Context) converter.DataConverter {
	options := getWorkflowEnvOptions(ctx)
	var dataConverter converter.DataConverter

	if options != nil && options.DataConverter != nil {
		dataConverter = options.DataConverter
	} else {
		dataConverter = converter.GetDefaultDataConverter()
	}

	return WithWorkflowContext(ctx, dataConverter)
}

func getRegistryFromWorkflowContext(ctx Context) *registry {
	env := getWorkflowEnvironment(ctx)
	return env.GetRegistry()
}

// getSignalChannel finds the associated channel for the signal.
func (w *WorkflowOptions) getSignalChannel(ctx Context, signalName string) ReceiveChannel {
	if ch, ok := w.signalChannels[signalName]; ok {
		return ch
	}
	ch := NewNamedBufferedChannel(ctx, signalName, defaultSignalChannelSize)
	w.signalChannels[signalName] = ch
	return ch
}

// GetUnhandledSignalNames returns signal names that have unconsumed signals.
func GetUnhandledSignalNames(ctx Context) []string {
	return getWorkflowEnvOptions(ctx).getUnhandledSignalNames()
}

// GetCurrentDetails gets the previously-set current details.
//
// NOTE: Experimental
func GetCurrentDetails(ctx Context) string {
	return getWorkflowEnvOptions(ctx).currentDetails
}

// SetCurrentDetails sets the current details.
//
// NOTE: Experimental
func SetCurrentDetails(ctx Context, details string) {
	getWorkflowEnvOptions(ctx).currentDetails = details
}

func getWorkflowMetadata(ctx Context) (*sdk.WorkflowMetadata, error) {
	info := GetWorkflowInfo(ctx)
	eo := getWorkflowEnvOptions(ctx)
	ret := &sdk.WorkflowMetadata{
		Definition: &sdk.WorkflowDefinition{
			Type: info.WorkflowType.Name,
			QueryDefinitions: []*sdk.WorkflowInteractionDefinition{
				{
					Name:        QueryTypeStackTrace,
					Description: "Current stack trace",
				},
				{
					Name:        QueryTypeOpenSessions,
					Description: "Open sessions on the workflow",
				},
				{
					Name:        QueryTypeWorkflowMetadata,
					Description: "Metadata about the workflow",
				},
			},
		},
		CurrentDetails: eo.currentDetails,
	}
	// Queries
	for k, v := range eo.queryHandlers {
		ret.Definition.QueryDefinitions = append(ret.Definition.QueryDefinitions, &sdk.WorkflowInteractionDefinition{
			Name:        k,
			Description: v.options.Description,
		})
	}
	// Signals
	for k, v := range eo.requestedSignalChannels {
		ret.Definition.SignalDefinitions = append(ret.Definition.SignalDefinitions, &sdk.WorkflowInteractionDefinition{
			Name:        k,
			Description: v.options.Description,
		})
	}
	// Updates
	for k, v := range eo.updateHandlers {
		ret.Definition.UpdateDefinitions = append(ret.Definition.UpdateDefinitions, &sdk.WorkflowInteractionDefinition{
			Name:        k,
			Description: v.description,
		})
	}
	// Sort interaction definitions
	sortWorkflowInteractionDefinitions(ret.Definition.QueryDefinitions)
	sortWorkflowInteractionDefinitions(ret.Definition.SignalDefinitions)
	sortWorkflowInteractionDefinitions(ret.Definition.UpdateDefinitions)
	return ret, nil
}

func sortWorkflowInteractionDefinitions(defns []*sdk.WorkflowInteractionDefinition) {
	sort.Slice(defns, func(i, j int) bool { return defns[i].Name < defns[j].Name })
}

// getUnhandledSignalNames returns signal names that have unconsumed signals.
func (w *WorkflowOptions) getUnhandledSignalNames() []string {
	var unhandledSignals []string
	for k, c := range w.signalChannels {
		ch := c.(*channelImpl)
		v, ok, _ := ch.receiveAsyncImpl(nil)
		if ok {
			unhandledSignals = append(unhandledSignals, k)
			ch.recValue = &v
		}
	}
	return unhandledSignals
}

func (w *WorkflowOptions) getRunningUpdateHandles() map[string]UpdateInfo {
	return w.runningUpdatesHandles
}

func (d *decodeFutureImpl) Get(ctx Context, valuePtr interface{}) error {
	more := d.futureImpl.channel.Receive(ctx, nil)
	if more {
		panic("not closed")
	}
	if !d.futureImpl.ready {
		panic("not ready")
	}
	if d.futureImpl.err != nil || d.futureImpl.value == nil || valuePtr == nil {
		return d.futureImpl.err
	}
	rf := reflect.ValueOf(valuePtr)
	if rf.Type().Kind() != reflect.Ptr {
		return errors.New("valuePtr parameter is not a pointer")
	}
	dataConverter := d.dataConverter
	if dataConverter == nil {
		dataConverter = getDataConverterFromWorkflowContext(ctx)
	}
	err := dataConverter.FromPayloads(d.futureImpl.value.(*commonpb.Payloads), valuePtr)
	if err != nil {
		return err
	}
	return d.futureImpl.err
}

// newDecodeFuture creates a new future as well as associated Settable that is used to set its value.
// fn - the decoded value needs to be validated against a function.
func newDecodeFuture(ctx Context, fn interface{}) (Future, Settable) {
	impl := &decodeFutureImpl{
		&futureImpl{channel: NewChannel(ctx).(*channelImpl)}, fn, nil}
	return impl, impl
}

// setQueryHandler sets query handler for given queryType.
func setQueryHandler(ctx Context, queryType string, handler interface{}, options QueryHandlerOptions) error {
	eo := getWorkflowEnvOptions(ctx)
	dataConverter := getDataConverterFromWorkflowContext(ctx)
	qh := &queryHandler{
		fn:            handler,
		queryType:     queryType,
		dataConverter: dataConverter,
		options:       options,
	}
	err := validateQueryHandlerFn(qh.fn)
	if err != nil {
		return err
	}

	eo.queryHandlers[queryType] = qh
	return nil
}

// setUpdateHandler sets update handler for a given update name.
func setUpdateHandler(ctx Context, updateName string, handler interface{}, opts UpdateHandlerOptions) error {
	uh, err := newUpdateHandler(updateName, handler, opts)
	if err != nil {
		return err
	}
	eo := getWorkflowEnvOptions(ctx)
	// Data and Failure converter wrapped with WorkflowSerializationContext in newWorkflowExecutionEventHandler.
	uh.dataConverter = getWorkflowEnvironment(ctx).GetDataConverter()
	uh.failureConverter = getWorkflowEnvironment(ctx).GetFailureConverter()
	eo.updateHandlers[updateName] = uh
	if getWorkflowEnvironment(ctx).TryUse(SDKPriorityUpdateHandling) {
		getWorkflowEnvironment(ctx).HandleQueuedUpdates(updateName)
		state := getState(ctx)
		defer state.unblocked()
		state.yield("letting any updates waiting on a handler run")
	}
	return nil
}

// validateEquivalentParams verifies that both arguments are functions and that
// said functions take the exact same parameter types in the same order but not
// considering the presence or absence of a workflow.Context parameter in the
// zeroth position.
func validateEquivalentParams(fn1, fn2 interface{}) error {
	fn1Type := reflect.TypeOf(fn1)
	fn2Type := reflect.TypeOf(fn2)

	if fn1Type.Kind() != reflect.Func {
		return fmt.Errorf("type must be function but was %s", fn1Type.Kind())
	}

	if fn2Type.Kind() != reflect.Func {
		return fmt.Errorf("type must be function but was %s", fn1Type.Kind())
	}

	ctxType := reflect.TypeOf(new(Context)).Elem()
	extractRelevantParamTypes := func(t reflect.Type) []reflect.Type {
		out := make([]reflect.Type, 0, t.NumIn())
		for i := 0; i < t.NumIn(); i++ {
			paramType := t.In(i)
			if i == 0 && paramType.Implements(ctxType) {
				// ignore the presence of a workflow.Context as a first param
				continue
			}
			out = append(out, paramType)
		}
		return out
	}

	fn1ParamTypes := extractRelevantParamTypes(fn1Type)
	fn2ParamTypes := extractRelevantParamTypes(fn2Type)

	if len(fn1ParamTypes) != len(fn2ParamTypes) {
		return errors.New("functions have different numbers of parameters")
	}

	for i := 0; i < len(fn1ParamTypes); i++ {
		fn1ParamType := fn1ParamTypes[i]
		fn2ParamType := fn2ParamTypes[i]
		if fn1ParamType != fn2ParamType {
			return fmt.Errorf("functions differ at parameter %v; %v != %v", i, fn1ParamType, fn2ParamType)
		}
	}
	return nil
}

func validateQueryHandlerFn(fn interface{}) error {
	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("handler must be function but was %s", fnType.Kind())
	}

	if fnType.NumOut() != 2 {
		return fmt.Errorf(
			"handler must return 2 values (serializable result and error), but found %d return values", fnType.NumOut(),
		)
	}

	if !isValidResultType(fnType.Out(0)) {
		return fmt.Errorf(
			"first return value of handler must be serializable but found: %v", fnType.Out(0).Kind(),
		)
	}
	if !isError(fnType.Out(1)) {
		return fmt.Errorf(
			"second return value of handler must be error but found %v", fnType.Out(fnType.NumOut()-1).Kind(),
		)
	}
	return nil
}

func (h *queryHandler) execute(input []interface{}) (result interface{}, err error) {
	// if query handler panic, convert it to error
	defer func() {
		if p := recover(); p != nil {
			result = nil
			st := getStackTraceRaw("query handler [panic]:", 7, 0)
			if p == panicIllegalAccessCoroutineState {
				// query handler code try to access workflow functions outside of workflow context, make error message
				// more descriptive and clear.
				p = "query handler must not use temporal context to do things like workflow.NewChannel(), " +
					"workflow.Go() or to call any workflow blocking functions like Channel.Get() or Future.Get()"
			}
			err = fmt.Errorf("query handler panic: %v, stack trace: %v", p, st)
		}
	}()

	return executeFunction(h.fn, input)
}

// Add adds delta, which may be negative, to the WaitGroup counter.
// If the counter becomes zero, all goroutines blocked on Wait are released.
// If the counter goes negative, Add panics.
//
// Note that calls with a positive delta that occur when the counter is zero
// must happen before a Wait. Calls with a negative delta, or calls with a
// positive delta that start when the counter is greater than zero, may happen
// at any time.
// Typically this means the calls to Add should execute before the statement
// creating the goroutine or other event to be waited for.
// If a WaitGroup is reused to wait for several independent sets of events,
// new Add calls must happen after all previous Wait calls have returned.
//
// param delta int -> the value to increment the WaitGroup counter by
func (wg *waitGroupImpl) Add(delta int) {
	wg.n = wg.n + delta
	if wg.n < 0 {
		panic("negative WaitGroup counter")
	}
	if (wg.n > 0) || (!wg.waiting) {
		return
	}
	if wg.n == 0 {
		wg.settable.Set(false, nil)
	}
}

// Done decrements the WaitGroup counter by 1, indicating
// that a coroutine in the WaitGroup has completed
func (wg *waitGroupImpl) Done() {
	wg.Add(-1)
}

// Wait blocks and waits for specified number of coroutines to
// finish executing and then unblocks once the counter has reached 0.
//
// param ctx Context -> workflow context
func (wg *waitGroupImpl) Wait(ctx Context) {
	assertNotInReadOnlyState(ctx)
	if wg.n <= 0 {
		return
	}
	if wg.waiting {
		panic("WaitGroup is reused before previous Wait has returned")
	}

	wg.waiting = true
	if err := wg.future.Get(ctx, &wg.waiting); err != nil {
		panic(err)
	}
	wg.future, wg.settable = NewFuture(ctx)
}

func (wg *waitGroupImpl) Go(ctx Context, f func(Context)) {
	wg.Add(1)
	Go(ctx, func(ctx Context) {
		defer wg.Done()
		f(ctx)
	})
}

// Spawn starts a new coroutine with Dispatcher.NewCoroutine
func (us updateSchedulerImpl) Spawn(ctx Context, name string, highPriority bool, f func(Context)) Context {
	return us.dispatcher.NewCoroutine(ctx, name, highPriority, f)
}

// Yield calls the yield function on the coroutineState associated with the
// supplied workflow context.
func (us updateSchedulerImpl) Yield(ctx Context, reason string) {
	getState(ctx).yield(reason)
}

func (m *mutexImpl) Lock(ctx Context) error {
	err := Await(ctx, func() bool {
		return !m.locked
	})
	if err != nil {
		return err
	}
	m.locked = true
	return nil
}

func (m *mutexImpl) TryLock(ctx Context) bool {
	assertNotInReadOnlyState(ctx)
	if m.locked {
		return false
	}
	m.locked = true
	return true
}

func (m *mutexImpl) Unlock() {
	if !m.locked {
		panic("Mutex.Unlock() was called on an unlocked mutex")
	}
	m.locked = false
}

func (m *mutexImpl) IsLocked() bool {
	return m.locked
}

func (s *semaphoreImpl) Acquire(ctx Context, n int64) error {
	err := Await(ctx, func() bool {
		return s.size-s.cur >= n
	})
	if err != nil {
		return err
	}
	s.cur += n
	return nil
}

func (s *semaphoreImpl) TryAcquire(ctx Context, n int64) bool {
	assertNotInReadOnlyState(ctx)
	success := s.size-s.cur >= n
	if success {
		s.cur += n
	}
	return success
}

func (s *semaphoreImpl) Release(n int64) {
	s.cur -= n
	if s.cur < 0 {
		panic("Semaphore.Release() released more than held")
	}
}

func incrementWorkflowTaskFailureCounter(metricsHandler metrics.Handler, failureReason string) {
	metricsHandler.WithTags(metrics.WorkflowTaskFailedTags(failureReason)).Counter(metrics.WorkflowTaskExecutionFailureCounter).Inc(1)
}
