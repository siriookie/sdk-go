package internal

// All code in this file is private to the package.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/internal/common/serializer"
	"go.temporal.io/sdk/internal/common/util"
	"go.temporal.io/sdk/internal/extstore"
	ilog "go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/log"
)

const (
	// Set to 2 pollers for now, can adjust later if needed. The typical RTT (round-trip time) is below 1ms within data
	// center. And the poll API latency is about 5ms. With 2 poller, we could achieve around 300~400 RPS.
	defaultConcurrentPollRoutineSize = 2

	defaultAutoscalingInitialNumberOfPollers = 5   // Default initial number of pollers when using autoscaling.
	defaultAutoscalingMinimumNumberOfPollers = 1   // Default minimum number of pollers when using autoscaling.
	defaultAutoscalingMaximumNumberOfPollers = 100 // Default maximum number of pollers when using autoscaling.

	defaultMaxConcurrentActivityExecutionSize = 1000   // Large concurrent activity execution size (1k)
	defaultWorkerActivitiesPerSecond          = 100000 // Large activity executions/sec (unlimited)

	defaultMaxConcurrentLocalActivityExecutionSize = 1000   // Large concurrent activity execution size (1k)
	defaultWorkerLocalActivitiesPerSecond          = 100000 // Large activity executions/sec (unlimited)

	defaultTaskQueueActivitiesPerSecond = 100000.0 // Large activity executions/sec (unlimited)

	defaultMaxConcurrentTaskExecutionSize = 1000   // hardcoded max task execution size.
	defaultWorkerTaskExecutionRate        = 100000 // Large task execution rate (unlimited)

	defaultPollerRate = 1000

	defaultMaxConcurrentSessionExecutionSize = 1000 // Large concurrent session execution size (1k)

	defaultDeadlockDetectionTimeout = time.Second // By default kill workflow tasks that are running more than 1 sec.
	// Unlimited deadlock detection timeout is used when we want to allow workflow tasks to run indefinitely, such
	// as during debugging.
	unlimitedDeadlockDetectionTimeout = math.MaxInt64

	testTagsContextKey = "temporal-testTags"
)

type (
	// WorkflowWorker wraps the code for hosting workflow types.
	// And worker is mapped 1:1 with task queue. If the user want's to poll multiple
	// task queue names they might have to manage 'n' workers for 'n' task queues.
	workflowWorker struct {
		executionParameters workerExecutionParameters
		workflowService     workflowservice.WorkflowServiceClient
		worker              *baseWorker
		localActivityWorker *baseWorker
		identity            string
		stopC               chan struct{}
		localActivityStopC  chan struct{}
		stickyUUID          string // Used for ShutdownWorker call
	}

	// ActivityWorker wraps the code for hosting activity types.
	// TODO: Worker doing heartbeating automatically while activity task is running
	activityWorker struct {
		executionParameters workerExecutionParameters
		workflowService     workflowservice.WorkflowServiceClient
		poller              taskPoller
		worker              *baseWorker
		identity            string
		stopC               chan struct{}
	}

	// sessionWorker wraps the code for hosting session creation, completion and
	// activities within a session. The creationWorker polls from a global taskqueue,
	// while the activityWorker polls from a resource specific taskqueue.
	sessionWorker struct {
		creationWorker *activityWorker
		activityWorker *activityWorker
	}

	// Worker overrides.
	workerOverrides struct {
		workflowTaskHandler WorkflowTaskHandler
		activityTaskHandler ActivityTaskHandler
		slotSupplier        SlotSupplier
	}

	// workerExecutionParameters defines worker configure/execution options.
	workerExecutionParameters struct {
		// Namespace name.
		Namespace string

		// Task queue name to poll.
		TaskQueue string

		// The tuner for the worker.
		Tuner WorkerTuner

		// Defines rate limiting on number of activity tasks that can be executed per second per worker.
		WorkerActivitiesPerSecond float64

		// Defines rate limiting on number of local activities that can be executed per second per worker.
		WorkerLocalActivitiesPerSecond float64

		// TaskQueueActivitiesPerSecond is the throttling limit for activity tasks controlled by the server.
		TaskQueueActivitiesPerSecond float64

		// User can provide an identity for the debuggability. If not provided the framework has
		// a default option.
		Identity string

		// The worker's build ID used for versioning, if one was set.
		//
		// Deprecated: use DeploymentOptions.Version for versioning instead.
		WorkerBuildID string

		// If true the worker is opting in to build ID based versioning.
		//
		// Deprecated: use DeploymentOptions.UseVersioning for versioning instead.
		UseBuildIDForVersioning bool

		// Worker deployment options containing all deployment versioning configuration.
		DeploymentOptions WorkerDeploymentOptions

		MetricsHandler metrics.Handler

		Logger log.Logger

		// Enable logging in replay mode
		EnableLoggingInReplay bool

		// Context to store user provided key/value pairs
		BackgroundContext context.Context

		// Context cancel function to cancel user context
		BackgroundContextCancel context.CancelCauseFunc

		StickyScheduleToStartTimeout time.Duration

		// WorkflowPanicPolicy is used for configuring how client's workflow task handler deals with workflow
		// code panicking which includes non backwards compatible changes to the workflow code without appropriate
		// versioning (see workflow.GetVersion).
		// The default behavior is to block workflow execution until the problem is fixed.
		WorkflowPanicPolicy WorkflowPanicPolicy

		DataConverter converter.DataConverter

		FailureConverter converter.FailureConverter

		// WorkerStopTimeout is the time delay before hard terminate worker
		WorkerStopTimeout time.Duration

		// WorkerStopChannel is a read only channel listen on worker close. The worker will close the channel before exit.
		WorkerStopChannel <-chan struct{}

		// WorkerFatalErrorCallback is a callback for fatal errors that should stop
		// the worker.
		WorkerFatalErrorCallback func(error)

		// SessionResourceID is a unique identifier of the resource the session will consume
		SessionResourceID string

		ContextPropagators []ContextPropagator

		// DeadlockDetectionTimeout specifies workflow task timeout.
		DeadlockDetectionTimeout time.Duration

		DefaultHeartbeatThrottleInterval time.Duration

		MaxHeartbeatThrottleInterval time.Duration

		// WorkflowTaskPollerBehavior defines the behavior of the workflow task poller.
		WorkflowTaskPollerBehavior PollerBehavior

		// ActivityTaskPollerBehavior defines the behavior of the activity task poller.
		ActivityTaskPollerBehavior PollerBehavior

		// NexusTaskPollerBehavior defines the behavior of the nexus task poller.
		NexusTaskPollerBehavior PollerBehavior

		// Pointer to the shared worker cache
		cache *WorkerCache

		eagerActivityExecutor *eagerActivityExecutor

		capabilities *workflowservice.GetSystemInfoResponse_Capabilities

		pollTimeTracker *pollTimeTracker

		workerInstanceKey string

		workerPollCompleteOnShutdown *atomic.Bool

		// Set to true during start() when the namespace has the poller_autoscaling capability.
		serverSupportsAutoscaling *atomic.Bool

		inboundPayloadVisitor PayloadVisitor

		outboundPayloadVisitor PayloadVisitor

		payloadVisitorConcurrency int

		setErrorLimits func(*payloadLimits)
	}

	// HistoryJSONOptions are options for HistoryFromJSON.
	HistoryJSONOptions struct {
		// LastEventID, if set, will only load history up to this ID (inclusive).
		LastEventID int64
	}

	// Represents the version of a specific worker deployment.
	//
	// Exposed as: [go.temporal.io/sdk/worker.WorkerDeploymentVersion]
	WorkerDeploymentVersion struct {
		// The name of the deployment this worker version belongs to
		DeploymentName string
		// The build id specific to this worker
		BuildID string
	}
)

var debugMode = os.Getenv("TEMPORAL_DEBUG") != ""

// newWorkflowWorker returns an instance of the workflow worker.
func newWorkflowWorker(client *WorkflowClient, params workerExecutionParameters, ppMgr pressurePointMgr, registry *registry) *workflowWorker {
	return newWorkflowWorkerInternal(client, params, ppMgr, nil, registry)
}

func ensureRequiredParams(params *workerExecutionParameters) {
	if params.Identity == "" {
		params.Identity = getWorkerIdentity(params.TaskQueue)
	}
	if params.Logger == nil {
		// create default logger if user does not supply one (should happen in tests only).
		params.Logger = ilog.NewDefaultLogger()
	}
	if params.MetricsHandler == nil {
		params.MetricsHandler = metrics.NopHandler
		params.Logger.Info("No metrics handler configured for temporal worker. Use NopHandler as default.")
	}
	if params.DataConverter == nil {
		params.DataConverter = converter.GetDefaultDataConverter()
		params.Logger.Info("No DataConverter configured for temporal worker. Use default one.")
	}
	if params.FailureConverter == nil {
		params.FailureConverter = GetDefaultFailureConverter()
	}
	if params.Tuner == nil {
		// Err cannot happen since these slot numbers are guaranteed valid
		params.Tuner, _ = NewFixedSizeTuner(
			FixedSizeTunerOptions{
				NumWorkflowSlots:      defaultMaxConcurrentTaskExecutionSize,
				NumActivitySlots:      defaultMaxConcurrentActivityExecutionSize,
				NumLocalActivitySlots: defaultMaxConcurrentLocalActivityExecutionSize,
				NumNexusSlots:         defaultMaxConcurrentTaskExecutionSize,
			})
	}
	if params.pollTimeTracker == nil {
		params.pollTimeTracker = &pollTimeTracker{}
	}
}

// getBuildID returns either the user-defined build ID if it was provided, or an autogenerated one
// using getBinaryChecksum
func (params *workerExecutionParameters) getBuildID() string {
	if params.WorkerBuildID != "" {
		return params.WorkerBuildID
	}
	return getBinaryChecksum()
}

// Returns true if this worker is part of our system namespace or per-namespace system task queue
func (params *workerExecutionParameters) isInternalWorker() bool {
	return params.Namespace == "temporal-system" || params.TaskQueue == "temporal-sys-per-ns-tq"
}

func newWorkflowWorkerInternal(client *WorkflowClient, params workerExecutionParameters, ppMgr pressurePointMgr, overrides *workerOverrides, registry *registry) *workflowWorker {
	workerStopChannel := make(chan struct{})
	params.WorkerStopChannel = getReadOnlyChannel(workerStopChannel)
	// Get a workflow task handler.
	ensureRequiredParams(&params)
	var taskHandler WorkflowTaskHandler
	if overrides != nil && overrides.workflowTaskHandler != nil {
		taskHandler = overrides.workflowTaskHandler
	} else {
		taskHandler = newWorkflowTaskHandler(params, ppMgr, registry)
	}
	return newWorkflowTaskWorkerInternal(taskHandler, taskHandler, client, params, workerStopChannel, registry.interceptors)
}

func newWorkflowTaskWorkerInternal(
	taskHandler WorkflowTaskHandler,
	contextManager WorkflowContextManager,
	client *WorkflowClient,
	params workerExecutionParameters,
	stopC chan struct{},
	interceptors []WorkerInterceptor,
) *workflowWorker {
	ensureRequiredParams(&params)
	var service workflowservice.WorkflowServiceClient
	if client != nil {
		service = client.workflowService
	}
	// Generate stickyUUID here so it can be stored in workflowWorker for ShutdownWorker call
	stickyUUID := uuid.NewString()
	taskProcessor := newWorkflowTaskProcessor(taskHandler, contextManager, service, params, stickyUUID)

	var scalableTaskPollers []scalableTaskPoller
	switch params.WorkflowTaskPollerBehavior.(type) {
	case *pollerBehaviorSimpleMaximum:
		scalableTaskPollers = []scalableTaskPoller{
			newScalableTaskPoller(
				taskProcessor.createPoller(Mixed),
				params.Logger,
				params.WorkflowTaskPollerBehavior,
				metrics.PollerTypeWorkflowTask,
				params.serverSupportsAutoscaling,
			),
		}

	case *pollerBehaviorAutoscaling:
		scalableTaskPollers = []scalableTaskPoller{
			newScalableTaskPoller(
				taskProcessor.createPoller(NonSticky),
				params.Logger,
				params.WorkflowTaskPollerBehavior,
				metrics.PollerTypeWorkflowTask,
				params.serverSupportsAutoscaling,
			),
		}

		if taskProcessor.stickyCacheSize > 0 {
			scalableTaskPollers = append(
				scalableTaskPollers,
				newScalableTaskPoller(
					taskProcessor.createPoller(Sticky),
					params.Logger,
					params.WorkflowTaskPollerBehavior,
					metrics.PollerTypeWorkflowStickyTask,
					params.serverSupportsAutoscaling,
				),
			)
		}
	}

	bwo := baseWorkerOptions{
		pollerRate:                   defaultPollerRate,
		slotSupplier:                 params.Tuner.GetWorkflowTaskSlotSupplier(),
		maxTaskPerSecond:             defaultWorkerTaskExecutionRate,
		taskPollers:                  scalableTaskPollers,
		taskProcessor:                taskProcessor,
		workerType:                   "WorkflowWorker",
		identity:                     params.Identity,
		buildId:                      params.getBuildID(),
		deploymentOptions:            params.DeploymentOptions,
		logger:                       params.Logger,
		stopTimeout:                  params.WorkerStopTimeout,
		fatalErrCb:                   params.WorkerFatalErrorCallback,
		metricsHandler:               params.MetricsHandler,
		workerPollCompleteOnShutdown: params.workerPollCompleteOnShutdown,
		slotReservationData: slotReservationData{
			taskQueue: params.TaskQueue,
		},
	}

	worker := newBaseWorker(bwo)

	// We want a separate stop channel for local activities because when a worker shuts down,
	// we need to allow pending local activities to finish running for that workflow task.
	// After all pending local activities are handled, we then close the local activity stop channel.
	laStopChannel := make(chan struct{})
	laParams := params
	laParams.WorkerStopChannel = laStopChannel

	// laTunnel is the glue that hookup 3 parts
	laTunnel := newLocalActivityTunnel(getReadOnlyChannel(laStopChannel))

	// 1) workflow handler will send local activity task to laTunnel
	if handlerImpl, ok := taskHandler.(*workflowTaskHandlerImpl); ok {
		handlerImpl.laTunnel = laTunnel
	}

	// 2) local activity task poller will poll from laTunnel, and result will be pushed to laTunnel
	localActivityTaskPoller := newLocalActivityPoller(laParams, laTunnel, interceptors, client, stopC)
	localActivityWorker := newBaseWorker(baseWorkerOptions{
		slotSupplier:     laParams.Tuner.GetLocalActivitySlotSupplier(),
		maxTaskPerSecond: laParams.WorkerLocalActivitiesPerSecond,
		taskPollers: []scalableTaskPoller{
			newScalableTaskPoller(
				localActivityTaskPoller,
				params.Logger,
				NewPollerBehaviorSimpleMaximum(
					PollerBehaviorSimpleMaximumOptions{
						MaximumNumberOfPollers: 2,
					},
				),
				"",
				nil,
			),
		},
		taskProcessor:  localActivityTaskPoller,
		workerType:     "LocalActivityWorker",
		identity:       laParams.Identity,
		buildId:        laParams.getBuildID(),
		logger:         laParams.Logger,
		stopTimeout:    laParams.WorkerStopTimeout,
		fatalErrCb:     laParams.WorkerFatalErrorCallback,
		metricsHandler: laParams.MetricsHandler,
		slotReservationData: slotReservationData{
			taskQueue: params.TaskQueue,
		},
	},
	)

	// 3) the result pushed to laTunnel will be sent as task to workflow worker to process.
	worker.taskQueueCh = laTunnel.resultCh

	return &workflowWorker{
		executionParameters: params,
		workflowService:     service,
		worker:              worker,
		localActivityWorker: localActivityWorker,
		identity:            params.Identity,
		stopC:               stopC,
		localActivityStopC:  laStopChannel,
		stickyUUID:          stickyUUID,
	}
}

// Start the worker.
func (ww *workflowWorker) Start() error {
	ww.localActivityWorker.Start()
	ww.worker.Start()
	return nil // TODO: propagate error
}

// Stop the worker.
func (ww *workflowWorker) Stop() {
	close(ww.stopC)
	// TODO: remove the stop methods in favor of the workerStopChannel
	ww.worker.Stop()
	close(ww.localActivityStopC)
	ww.localActivityWorker.Stop()
}

func newSessionWorker(client *WorkflowClient, params workerExecutionParameters, env *registry, maxConcurrentSessionExecutionSize int) *sessionWorker {
	if params.Identity == "" {
		params.Identity = getWorkerIdentity(params.TaskQueue)
	}
	// For now resourceID is hidden from user so we will always create a unique one for each worker.
	if params.SessionResourceID == "" {
		params.SessionResourceID = uuid.NewString()
	}
	sessionEnvironment := newSessionEnvironment(params.SessionResourceID, maxConcurrentSessionExecutionSize)

	creationTaskqueue := getCreationTaskqueue(params.TaskQueue)
	params.BackgroundContext = context.WithValue(params.BackgroundContext, sessionEnvironmentContextKey, sessionEnvironment)
	params.TaskQueue = sessionEnvironment.GetResourceSpecificTaskqueue()
	// For the resource specific task queue, we don't need to include deployment options
	// Save them to restore later
	deployments := params.DeploymentOptions
	useBuildIDForVersioning := params.UseBuildIDForVersioning
	// Disable versioning for activity worker within session, but still send deployment name for debug purpose
	params.DeploymentOptions.UseVersioning = false
	params.UseBuildIDForVersioning = false
	activityWorker := newActivityWorker(client, params,
		&workerOverrides{
			slotSupplier: params.Tuner.GetSessionActivitySlotSupplier(),
		}, env, nil)

	params.ActivityTaskPollerBehavior = NewPollerBehaviorSimpleMaximum(
		PollerBehaviorSimpleMaximumOptions{
			MaximumNumberOfPollers: 1,
		},
	)
	params.TaskQueue = creationTaskqueue
	params.DeploymentOptions = deployments
	params.UseBuildIDForVersioning = useBuildIDForVersioning
	// Although we have session token bucket to limit session size across creation
	// and recreation, we also limit it here for creation only
	overrides := &workerOverrides{}
	overrides.slotSupplier, _ = NewFixedSizeSlotSupplier(maxConcurrentSessionExecutionSize)
	creationWorker := newActivityWorker(client, params, overrides, env, sessionEnvironment.GetTokenBucket())

	return &sessionWorker{
		creationWorker: creationWorker,
		activityWorker: activityWorker,
	}
}

func (sw *sessionWorker) Start() error {
	err := sw.creationWorker.Start()
	if err != nil {
		return err
	}

	err = sw.activityWorker.Start()
	if err != nil {
		sw.creationWorker.Stop()
		return err
	}
	return nil
}

func (sw *sessionWorker) Stop() {
	sw.creationWorker.Stop()
	sw.activityWorker.Stop()
}

func (sw *sessionWorker) getCreationWorkerTaskQueue() string {
	return sw.creationWorker.executionParameters.TaskQueue
}

func (sw *sessionWorker) getActivityWorkerTaskQueue() string {
	return sw.activityWorker.executionParameters.TaskQueue
}

func (sw *sessionWorker) stopPolling() {
	sw.creationWorker.worker.stopPolling()
	sw.activityWorker.worker.stopPolling()
}

func newActivityWorker(
	client *WorkflowClient,
	params workerExecutionParameters,
	overrides *workerOverrides,
	env *registry,
	sessionTokenBucket *sessionTokenBucket,
) *activityWorker {
	var service workflowservice.WorkflowServiceClient
	if client != nil {
		service = client.workflowService
	}
	workerStopChannel := make(chan struct{}, 1)
	params.WorkerStopChannel = getReadOnlyChannel(workerStopChannel)
	ensureRequiredParams(&params)

	// Get a activity task handler.
	var taskHandler ActivityTaskHandler
	if overrides != nil && overrides.activityTaskHandler != nil {
		taskHandler = overrides.activityTaskHandler
	} else {
		taskHandler = newActivityTaskHandler(client, params, env)
	}

	poller := newActivityTaskPoller(taskHandler, service, params)
	var slotSupplier SlotSupplier
	if overrides != nil && overrides.slotSupplier != nil {
		slotSupplier = overrides.slotSupplier
	} else {
		slotSupplier = params.Tuner.GetActivityTaskSlotSupplier()
	}
	bwo := baseWorkerOptions{
		pollerRate:       defaultPollerRate,
		slotSupplier:     slotSupplier,
		maxTaskPerSecond: params.WorkerActivitiesPerSecond,
		taskPollers: []scalableTaskPoller{
			newScalableTaskPoller(poller, params.Logger, params.ActivityTaskPollerBehavior, metrics.PollerTypeActivityTask, params.serverSupportsAutoscaling),
		},
		taskProcessor:                poller,
		workerType:                   "ActivityWorker",
		identity:                     params.Identity,
		buildId:                      params.getBuildID(),
		logger:                       params.Logger,
		stopTimeout:                  params.WorkerStopTimeout,
		fatalErrCb:                   params.WorkerFatalErrorCallback,
		backgroundContextCancel:      params.BackgroundContextCancel,
		metricsHandler:               params.MetricsHandler,
		sessionTokenBucket:           sessionTokenBucket,
		workerPollCompleteOnShutdown: params.workerPollCompleteOnShutdown,
		slotReservationData: slotReservationData{
			taskQueue: params.TaskQueue,
		},
	}

	base := newBaseWorker(bwo)
	return &activityWorker{
		executionParameters: params,
		workflowService:     service,
		worker:              base,
		poller:              poller,
		identity:            params.Identity,
		stopC:               workerStopChannel,
	}
}

// Start the worker.
func (aw *activityWorker) Start() error {
	aw.worker.Start()
	return nil // TODO: propagate errors
}

// Stop the worker.
func (aw *activityWorker) Stop() {
	close(aw.stopC)
	aw.worker.Stop()
}

type registry struct {
	sync.Mutex
	nexusServices                 map[string]*nexus.Service
	workflowFuncMap               map[string]interface{}
	workflowAliasMap              map[string]string
	workflowVersioningBehaviorMap map[string]VersioningBehavior
	activityFuncMap               map[string]activity
	activityAliasMap              map[string]string
	dynamicWorkflow               interface{}
	dynamicWorkflowOptions        DynamicRegisterWorkflowOptions
	dynamicActivity               activity
	_                             DynamicRegisterActivityOptions
	interceptors                  []WorkerInterceptor
}

type registryOptions struct {
	disableAliasing bool
}

func (r *registry) RegisterWorkflow(af interface{}) {
	r.RegisterWorkflowWithOptions(af, RegisterWorkflowOptions{})
}

func (r *registry) RegisterWorkflowWithOptions(
	wf interface{},
	options RegisterWorkflowOptions,
) {
	// Support direct registration of WorkflowDefinition
	factory, ok := wf.(WorkflowDefinitionFactory)
	if ok {
		if len(options.Name) == 0 {
			panic("WorkflowDefinitionFactory must be registered with a name")
		}
		if strings.HasPrefix(options.Name, temporalPrefix) {
			panic(temporalPrefixError)
		}
		r.Lock()
		defer r.Unlock()
		r.workflowFuncMap[options.Name] = factory
		r.workflowVersioningBehaviorMap[options.Name] = options.VersioningBehavior
		return
	}
	// Validate that it is a function
	fnType := reflect.TypeOf(wf)
	if err := validateFnFormat(fnType, true, false); err != nil {
		panic(err)
	}
	fnName, _ := getFunctionName(wf)
	alias := options.Name
	registerName := fnName
	if len(alias) > 0 {
		registerName = alias
	}

	if strings.HasPrefix(alias, temporalPrefix) || strings.HasPrefix(registerName, temporalPrefix) {
		panic(temporalPrefixError)
	}

	r.Lock()
	defer r.Unlock()

	if !options.DisableAlreadyRegisteredCheck {
		if _, ok := r.workflowFuncMap[registerName]; ok {
			panic(fmt.Sprintf("workflow name \"%v\" is already registered", registerName))
		}
	}
	r.workflowFuncMap[registerName] = wf
	r.workflowVersioningBehaviorMap[registerName] = options.VersioningBehavior

	if len(alias) > 0 && r.workflowAliasMap != nil {
		r.workflowAliasMap[fnName] = alias
	}
}

func (r *registry) RegisterDynamicWorkflow(wf interface{}, options DynamicRegisterWorkflowOptions) {
	r.Lock()
	defer r.Unlock()
	// Support direct registration of WorkflowDefinition
	factory, ok := wf.(WorkflowDefinitionFactory)
	if ok {
		r.dynamicWorkflow = factory
		r.dynamicWorkflowOptions = options
		return
	}

	// Validate that it is a function
	fnType := reflect.TypeOf(wf)
	if err := validateFnFormat(fnType, true, true); err != nil {
		panic(err)
	}
	if r.dynamicWorkflow != nil {
		panic("dynamic workflow already registered")
	}
	r.dynamicWorkflow = wf
	r.dynamicWorkflowOptions = options
}

func (r *registry) RegisterActivity(af interface{}) {
	r.RegisterActivityWithOptions(af, RegisterActivityOptions{})
}

func (r *registry) RegisterActivityWithOptions(
	af interface{},
	options RegisterActivityOptions,
) {
	// Support direct registration of activity
	a, ok := af.(activity)
	if ok {
		if options.Name == "" {
			panic("registration of activity interface requires name")
		}
		if strings.HasPrefix(options.Name, temporalPrefix) {
			panic(temporalPrefixError)
		}
		r.addActivityWithLock(options.Name, a)
		return
	}
	// Validate that it is a function
	fnType := reflect.TypeOf(af)
	if fnType.Kind() == reflect.Ptr && fnType.Elem().Kind() == reflect.Struct {
		registerErr := r.registerActivityStructWithOptions(af, options)
		if registerErr != nil {
			panic(registerErr)
		}
		return
	}
	if err := validateFnFormat(fnType, false, false); err != nil {
		panic(err)
	}
	fnName, _ := getFunctionName(af)
	alias := options.Name
	registerName := fnName
	if len(alias) > 0 {
		registerName = alias
	}

	if strings.HasPrefix(alias, temporalPrefix) || strings.HasPrefix(registerName, temporalPrefix) {
		panic(temporalPrefixError)
	}

	r.Lock()
	defer r.Unlock()

	if !options.DisableAlreadyRegisteredCheck {
		if _, ok := r.activityFuncMap[registerName]; ok {
			panic(fmt.Sprintf("activity type \"%v\" is already registered", registerName))
		}
	}
	r.activityFuncMap[registerName] = &activityExecutor{name: registerName, fn: af}
	if len(alias) > 0 && r.activityAliasMap != nil {
		r.activityAliasMap[fnName] = alias
	}
}

func (r *registry) registerActivityStructWithOptions(aStruct interface{}, options RegisterActivityOptions) error {
	r.Lock()
	defer r.Unlock()

	structValue := reflect.ValueOf(aStruct)
	structType := structValue.Type()
	count := 0
	for i := 0; i < structValue.NumMethod(); i++ {
		methodValue := structValue.Method(i)
		method := structType.Method(i)
		// skip private method
		if method.PkgPath != "" {
			continue
		}
		name := method.Name
		if err := validateFnFormat(method.Type, false, false); err != nil {
			if options.SkipInvalidStructFunctions {
				continue
			}

			return fmt.Errorf("method %s of %s: %w", name, structType.Name(), err)
		}
		registerName := options.Name + name
		if !options.DisableAlreadyRegisteredCheck {
			if _, ok := r.getActivityNoLock(registerName); ok {
				return fmt.Errorf("activity type \"%v\" is already registered", registerName)
			}
		}
		r.activityFuncMap[registerName] = &activityExecutor{name: registerName, fn: methodValue.Interface()}
		count++
	}
	if count == 0 {
		return fmt.Errorf("no activities (public methods) found at %v structure", structType.Name())
	}
	return nil
}

func (r *registry) RegisterDynamicActivity(af interface{}, options DynamicRegisterActivityOptions) {
	r.Lock()
	defer r.Unlock()
	// Support direct registration of activity
	a, ok := af.(activity)
	if ok {
		r.dynamicActivity = a
		return
	}
	// Validate that it is a function
	fnType := reflect.TypeOf(af)
	if err := validateFnFormat(fnType, false, true); err != nil {
		panic(err)
	}
	if r.dynamicActivity != nil {
		panic("dynamic activity already registered")
	}
	r.dynamicActivity = &activityExecutor{name: "", fn: af, dynamic: true}
}

func (r *registry) RegisterNexusService(service *nexus.Service) {
	if service.Name == "" {
		panic(fmt.Errorf("tried to register a service with no name"))
	}

	r.Lock()
	defer r.Unlock()

	if _, ok := r.nexusServices[service.Name]; ok {
		panic(fmt.Sprintf("service name \"%v\" is already registered", service.Name))
	}
	r.nexusServices[service.Name] = service
}

func (r *registry) getWorkflowAlias(fnName string) (string, bool) {
	r.Lock()
	defer r.Unlock()
	alias, ok := r.workflowAliasMap[fnName]
	return alias, ok
}

func (r *registry) getWorkflowFn(fnName string) (interface{}, bool) {
	r.Lock()
	defer r.Unlock()
	if fn, ok := r.workflowFuncMap[fnName]; ok {
		return fn, ok
	}

	if r.dynamicWorkflow != nil {
		return "dynamic", true
	}
	return nil, false
}

func (r *registry) getRegisteredWorkflowTypes() []string {
	r.Lock()
	defer r.Unlock()
	var result []string
	for t := range r.workflowFuncMap {
		result = append(result, t)
	}
	return result
}

func (r *registry) getActivityAlias(fnName string) (string, bool) {
	r.Lock()
	defer r.Unlock()
	alias, ok := r.activityAliasMap[fnName]
	return alias, ok
}

func (r *registry) addActivityWithLock(fnName string, a activity) {
	r.Lock()
	defer r.Unlock()
	r.activityFuncMap[fnName] = a
}

func (r *registry) GetActivity(fnName string) (activity, bool) {
	r.Lock()
	defer r.Unlock()
	if a, ok := r.activityFuncMap[fnName]; ok {
		return a, ok
	}
	if r.dynamicActivity != nil {
		return r.dynamicActivity, true
	}
	return nil, false
}

func (r *registry) getActivityNoLock(fnName string) (activity, bool) {
	a, ok := r.activityFuncMap[fnName]
	return a, ok
}

func (r *registry) getRegisteredActivities() []activity {
	r.Lock()
	defer r.Unlock()
	numActivities := len(r.activityFuncMap)
	if r.dynamicActivity != nil {
		numActivities++
	}
	activities := make([]activity, 0, numActivities)
	for _, a := range r.activityFuncMap {
		activities = append(activities, a)
	}
	if r.dynamicActivity != nil {
		activities = append(activities, r.dynamicActivity)
	}
	return activities
}

func (r *registry) getRegisteredActivityTypes() []string {
	r.Lock()
	defer r.Unlock()
	var result []string
	for name := range r.activityFuncMap {
		result = append(result, name)
	}
	return result
}

func (r *registry) getWorkflowDefinition(wt WorkflowType) (WorkflowDefinition, error) {
	// 上游 handleWorkflowExecutionStarted 会把 workflowInfo.WorkflowType 传进来；
	// workflowInfo.WorkflowType 来自当前 WorkflowExecutionStarted history event 的 workflow type。
	// 这里先用这个名字作为 registry 查找 key。
	lookup := wt.Name
	// RegisterWorkflowWithOptions 在 options.Name 非空且 alias map 启用时，
	// 会执行 workflowAliasMap[fnName] = options.Name，同时把 workflowFuncMap[options.Name] = wf。
	// 因此如果这里传入的是 Go 函数名 fnName，getWorkflowAlias 会把它转换成真正注册在 workflowFuncMap 里的 options.Name。
	// 如果 alias map 被 DisableRegistrationAliasing 关闭，workflowAliasMap 为 nil，这一步自然查不到 alias。
	if alias, ok := r.getWorkflowAlias(lookup); ok {
		lookup = alias
	}
	// getWorkflowFn 的实现顺序是：
	// 1. 先查 workflowFuncMap[lookup]，也就是 RegisterWorkflowWithOptions 写入的普通 workflow 或 factory；
	// 2. 如果普通表没命中，但 dynamicWorkflow 非空，返回字符串 "dynamic" 和 ok=true；
	// 3. 两者都没有才返回 nil,false。
	wf, ok := r.getWorkflowFn(lookup)
	if !ok {
		// 到这里说明 workflowFuncMap 没有 lookup，且没有注册 dynamic workflow。
		// supported 只列出 workflowFuncMap 中的固定 workflow type；dynamic workflow 不在这个列表里。
		// 这个错误会返回给 handleWorkflowExecutionStarted，再让当前 Workflow Task 按 SDK 错误路径失败。
		supported := strings.Join(r.getRegisteredWorkflowTypes(), ", ")
		return nil, fmt.Errorf("unable to find workflow type: %v. Supported types: [%v]", lookup, supported)
	}
	// RegisterWorkflowWithOptions 支持直接传入 WorkflowDefinitionFactory。
	// 这种 factory 会以 options.Name 作为 key 存进 workflowFuncMap。
	// WorkflowDefinitionFactory 接口注释要求 NewWorkflowDefinition 每次都返回新的 WorkflowDefinition；
	// 所以这里不再包装 workflowExecutor，而是直接让 factory 创建本次 workflow execution 的 definition。
	wdf, ok := wf.(WorkflowDefinitionFactory)
	if ok {
		return wdf.NewWorkflowDefinition(), nil
	}
	// dynamic 用来告诉 workflowExecutor.Execute 采用 dynamic workflow 的参数解码方式。
	// validateFnFormat(..., isWorkflow=true, isDynamic=true) 要求 dynamic workflow 形如：
	// func(workflow.Context, converter.EncodedValues) (..., error)
	// 所以 executor.Execute 在 dynamic=true 时不会按普通函数签名逐个解码参数，
	// 而是把整个 input 包成一个 EncodedValues 作为唯一业务参数传进去。
	var dynamic bool
	// getWorkflowFn 在 fallback 到 dynamic workflow 时返回字符串 "dynamic" 作为哨兵值，
	// 这里识别哨兵值后，替换成 RegisterDynamicWorkflow 保存到 r.dynamicWorkflow 的真实函数或 factory。
	if d, ok := wf.(string); ok && d == "dynamic" {
		wf = r.dynamicWorkflow
		dynamic = true
	}
	// workflowExecutor.Execute 是后续真正调用用户 workflow 函数的那层：
	// 普通 workflow 会用 decodeArgsToRawValues 按函数签名解码 input；
	// dynamic workflow 会把 input 包成 EncodedValues；
	// 然后通过 inboundInterceptor.ExecuteWorkflow 调用用户函数，并把返回值 encode 成 payload。
	executor := &workflowExecutor{workflowType: lookup, fn: wf, interceptors: r.interceptors, dynamic: dynamic}
	// newSyncWorkflowDefinition 只是把 workflowExecutor 包成 WorkflowDefinition。
	// handleWorkflowExecutionStarted 随后会调用 definition.Execute；
	// syncWorkflowDefinition.Execute 会创建 dispatcher/root coroutine，并在 root coroutine 里先 yield。
	// 真正驱动 dispatcher 运行用户 workflow 代码的是后续 WorkflowTaskStarted 事件触发的 OnWorkflowTaskStarted。
	return newSyncWorkflowDefinition(executor), nil
}

func (r *registry) getWorkflowVersioningBehavior(wt WorkflowType) (VersioningBehavior, bool) {
	lookup := wt.Name
	if alias, ok := r.getWorkflowAlias(lookup); ok {
		lookup = alias
	}
	r.Lock()
	defer r.Unlock()
	if behavior, ok := r.workflowVersioningBehaviorMap[lookup]; ok {
		return behavior, behavior != VersioningBehaviorUnspecified
	}
	if r.dynamicWorkflowOptions.LoadDynamicRuntimeOptions != nil {
		config := LoadDynamicRuntimeOptionsDetails{WorkflowType: wt}
		if behavior, err := r.dynamicWorkflowOptions.LoadDynamicRuntimeOptions(config); err == nil {
			return behavior.VersioningBehavior, true
		}
	}
	return VersioningBehaviorUnspecified, false
}

func (r *registry) getNexusService(service string) *nexus.Service {
	r.Lock()
	defer r.Unlock()
	return r.nexusServices[service]
}

func (r *registry) getRegisteredNexusServices() []*nexus.Service {
	r.Lock()
	defer r.Unlock()
	result := make([]*nexus.Service, 0, len(r.nexusServices))
	for _, s := range r.nexusServices {
		result = append(result, s)
	}
	return result
}

// Validate function parameters.
func validateFnFormat(fnType reflect.Type, isWorkflow, isDynamic bool) error {
	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("expected a func as input but was %s", fnType.Kind())
	}
	if isWorkflow {
		if fnType.NumIn() < 1 {
			return fmt.Errorf(
				"expected at least one argument of type workflow.Context in function, found %d input arguments",
				fnType.NumIn(),
			)
		}
		if !isWorkflowContext(fnType.In(0)) {
			return fmt.Errorf("expected first argument to be workflow.Context but found %s", fnType.In(0))
		}
	} else {
		// For activities, check that workflow context is not accidentally provided
		// Activities registered with structs will have their receiver as the first argument so confirm it is not
		// in the first two arguments
		for i := 0; i < fnType.NumIn() && i < 2; i++ {
			if isWorkflowContext(fnType.In(i)) {
				return fmt.Errorf("unexpected use of workflow context for an activity")
			}
		}
	}

	if isDynamic {
		if fnType.NumIn() != 2 {
			return fmt.Errorf(
				"expected function to have two arguments, first being workflow.Context and second being an EncodedValues type, found %d arguments", fnType.NumIn(),
			)
		}
		if fnType.In(1) != reflect.TypeOf((*converter.EncodedValues)(nil)).Elem() {
			return fmt.Errorf("expected function to EncodedValues as second argument, got %s", fnType.In(1).Elem())
		}
	}

	// Return values
	// We expect either
	// 	<result>, error
	//	(or) just error
	if fnType.NumOut() < 1 || fnType.NumOut() > 2 {
		return fmt.Errorf(
			"expected function to return result, error or just error, but found %d return values", fnType.NumOut(),
		)
	}
	if fnType.NumOut() > 1 && !isValidResultType(fnType.Out(0)) {
		return fmt.Errorf(
			"expected function first return value to return valid type but found: %v", fnType.Out(0).Kind(),
		)
	}
	if !isError(fnType.Out(fnType.NumOut() - 1)) {
		return fmt.Errorf(
			"expected function second return value to return error but found %v", fnType.Out(fnType.NumOut()-1).Kind(),
		)
	}
	return nil
}

func newRegistry() *registry { return newRegistryWithOptions(registryOptions{}) }

func newRegistryWithOptions(options registryOptions) *registry {
	r := &registry{
		workflowFuncMap:               make(map[string]interface{}),
		workflowVersioningBehaviorMap: make(map[string]VersioningBehavior),
		activityFuncMap:               make(map[string]activity),
		nexusServices:                 make(map[string]*nexus.Service),
	}
	if !options.disableAliasing {
		r.workflowAliasMap = make(map[string]string)
		r.activityAliasMap = make(map[string]string)
	}
	return r
}

// Wrapper to execute workflow functions.
type workflowExecutor struct {
	workflowType string
	fn           interface{}
	interceptors []WorkerInterceptor
	dynamic      bool
}

func (we *workflowExecutor) Execute(ctx Context, input *commonpb.Payloads) (*commonpb.Payloads, error) {
	dataConverter := WithWorkflowContext(ctx, getWorkflowEnvOptions(ctx).DataConverter)
	fnType := reflect.TypeOf(we.fn)

	var args []interface{}
	var err error
	if we.dynamic {
		// Dynamic workflows take in a single EncodedValues, encode all data into single EncodedValues
		args = []interface{}{newEncodedValues(input, dataConverter)}
	} else {
		args, err = decodeArgsToRawValues(dataConverter, fnType, input)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to decode the workflow function input payload with error: %w, function name: %v",
				err, we.workflowType)
		}
	}

	envInterceptor := getWorkflowEnvironmentInterceptor(ctx)
	envInterceptor.fn = we.fn

	// Execute and serialize result
	result, err := envInterceptor.inboundInterceptor.ExecuteWorkflow(ctx, &ExecuteWorkflowInput{Args: args})
	var serializedResult *commonpb.Payloads
	if err == nil && result != nil {
		serializedResult, err = encodeArg(dataConverter, result)
	}
	return serializedResult, err
}

// Wrapper to execute activity functions.
type activityExecutor struct {
	name             string
	fn               interface{}
	skipInterceptors bool
	dynamic          bool
}

func (ae *activityExecutor) ActivityType() ActivityType {
	return ActivityType{Name: ae.name}
}

func (ae *activityExecutor) GetFunction() interface{} {
	return ae.fn
}

func (ae *activityExecutor) Execute(ctx context.Context, input *commonpb.Payloads) (*commonpb.Payloads, error) {
	fnType := reflect.TypeOf(ae.fn)
	dataConverter := getDataConverterFromActivityCtx(ctx)

	var args []interface{}
	var err error
	if ae.dynamic {
		// Dynamic activities take in a single EncodedValues, encode all data into single EncodedValues
		args = []interface{}{newEncodedValues(input, dataConverter)}
	} else {
		args, err = decodeArgsToRawValues(dataConverter, fnType, input)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to decode the activity function input payload with error: %w for function name: %v",
				err, ae.name)
		}
	}

	return ae.ExecuteWithActualArgs(ctx, args)
}

func (ae *activityExecutor) ExecuteWithActualArgs(ctx context.Context, args []interface{}) (*commonpb.Payloads, error) {
	dataConverter := getDataConverterFromActivityCtx(ctx)

	envInterceptor := getActivityEnvironmentInterceptor(ctx)
	envInterceptor.fn = ae.fn

	// Execute and serialize result
	interceptor := envInterceptor.inboundInterceptor
	if ae.skipInterceptors {
		interceptor = envInterceptor
	}
	result, resultErr := interceptor.ExecuteActivity(ctx, &ExecuteActivityInput{Args: args})
	var serializedResult *commonpb.Payloads
	if result != nil {
		// As a special case, if the result is already a payload, just use it
		var ok bool
		if serializedResult, ok = result.(*commonpb.Payloads); !ok {
			var err error
			if serializedResult, err = encodeArg(dataConverter, result); err != nil {
				return nil, err
			}
		}
	}
	return serializedResult, resultErr
}

func getDataConverterFromActivityCtx(ctx context.Context) converter.DataConverter {
	var dataConverter converter.DataConverter

	env := getActivityEnvironmentFromCtx(ctx)
	if env != nil && env.dataConverter != nil {
		dataConverter = env.dataConverter
	} else {
		dataConverter = converter.GetDefaultDataConverter()
	}
	return WithContext(ctx, dataConverter)
}

func getActivityEnvironmentFromCtx(ctx context.Context) *activityEnvironment {
	if ctx == nil || ctx.Value(activityEnvContextKey) == nil {
		return nil
	}
	return ctx.Value(activityEnvContextKey).(*activityEnvironment)
}

// AggregatedWorker combines management of both workflowWorker and activityWorker worker lifecycle.
// AggregatedWorker 是 Temporal Go SDK 中 Worker 的核心聚合结构体。
// 它统一管理了 Workflow Worker、Activity Worker、Session Worker、Nexus Worker 这四种
// 子 Worker 的生命周期（创建、启动、停止），以及插件系统、心跳上报、错误处理等横切关注点。
//
// 设计意图：使用者通过 NewAggregatedWorker 创建一个 AggregatedWorker，然后只需要调用
// Start() / Stop() 即可控制所有子 Worker 的启停，无需分别操作。
type AggregatedWorker struct {
	// executionParams 保存了所有执行参数（namespace、taskqueue、logger、版本信息等），
	// 在 NewAggregatedWorker 中填充完成，传递给各个子 Worker 使用。
	// 注意：Nexus Worker 在 Start() 时才会被创建，所以需要保留此字段供那时使用。
	executionParams workerExecutionParameters

	// memoizedStart 是 Start() 的包装函数，内部使用 sync.OnceValue 确保：
	// 1. 无论 Start() 被调用多少次，start() 只会真正执行一次
	// 2. 多次调用返回相同的结果（包括 error）
	// 这样可以安全地在多个 goroutine 中并发调用 Start()。
	memoizedStart func() error

	client         *WorkflowClient // SDK 客户端，封装了与 Temporal Server 的 gRPC 连接
	workflowWorker *workflowWorker // Workflow Task 处理 Worker（可选，DisableWorkflowWorker=true 时为 nil）
	activityWorker *activityWorker // Activity Task 处理 Worker（可选，LocalActivityWorkerOnly=true 时为 nil）
	sessionWorker  *sessionWorker  // Session 内部 Worker（仅在 EnableSessionWorker=true 时创建）
	nexusWorker    *nexusWorker    // Nexus 协议 Worker（仅在有已注册 Nexus Service 时，在 Start() 中延迟创建）
	logger         log.Logger      // 带 namespace + taskqueue + workerID 标签的结构化日志
	registry       *registry       // Worker 级别的注册表：存储 Workflow/Activity/NexusService 的映射

	// started 在 start() 执行开始时即设置为 true，代表 Worker 已经启动（或正在启动中）
	started atomic.Bool
	// shuttingDown 在 Stop() 被调用时设置为 true，用于通知心跳回调等组件当前正在关闭
	shuttingDown atomic.Bool
	// stopC 是一个关闭后永不重置的 channel（close-once），所有子 Worker 的 goroutine
	// 通过 <-stopC 来感知"Worker 需要停止"的信号，实现统一的优雅关闭
	stopC        chan struct{}
	fatalErr     error      // 致命错误（来自 WorkerFatalErrorCallback 回调）
	fatalErrLock sync.Mutex // 保护 fatalErr 的读写互斥锁

	// capabilities 保存了 Temporal Server 的能力信息（如是否支持某些 protobuf 特性）。
	// 这是一个指针，因为 Worker 在创建时还没有连接 Server，只有在 Start() 中
	// 通过 loadCapabilities() 从 Server 获取后才填充，各子 Worker 通过解引用读取。
	capabilities *workflowservice.GetSystemInfoResponse_Capabilities

	workerInstanceKey     string                                      // Worker 实例的唯一标识符（UUID），用于心跳识别
	plugins               []WorkerPlugin                              // Worker 插件列表（含 Client 级别 + WorkerOptions 级别）
	pluginRegistryOptions *WorkerPluginConfigureWorkerRegistryOptions // 插件在 ConfigureWorker 阶段填充的注册选项（Never nil）

	heartbeatMetrics             *heartbeatMetricsHandler         // 心跳指标采集器（仅当 workerHeartbeatInterval != 0 时创建）
	heartbeatCallback            func() *workerpb.WorkerHeartbeat // 构造心跳 PB 消息的回调函数（在心跳 goroutine 中并发调用）
	workerPollCompleteOnShutdown *atomic.Bool                     // Server 是否支持"Shutdown 时完成当前 Poll"的标志
}

// RegisterWorkflow registers workflow implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterWorkflow(w interface{}) {
	if aw.workflowWorker == nil {
		panic("workflow worker disabled, cannot register workflow")
	}
	if aw.executionParams.UseBuildIDForVersioning &&
		(aw.executionParams.DeploymentOptions.Version != WorkerDeploymentVersion{}) &&
		aw.executionParams.DeploymentOptions.DefaultVersioningBehavior == VersioningBehaviorUnspecified {
		panic("workflow type does not have a versioning behavior")
	}
	if aw.pluginRegistryOptions.OnRegisterWorkflow != nil {
		aw.pluginRegistryOptions.OnRegisterWorkflow(w, RegisterWorkflowOptions{})
	}
	aw.registry.RegisterWorkflow(w)
}

// RegisterWorkflowWithOptions registers workflow implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterWorkflowWithOptions(w interface{}, options RegisterWorkflowOptions) {
	if aw.workflowWorker == nil {
		panic("workflow worker disabled, cannot register workflow")
	}
	if options.VersioningBehavior == VersioningBehaviorUnspecified &&
		(aw.executionParams.DeploymentOptions.Version != WorkerDeploymentVersion{}) &&
		aw.executionParams.UseBuildIDForVersioning &&
		aw.executionParams.DeploymentOptions.DefaultVersioningBehavior == VersioningBehaviorUnspecified {
		panic("workflow type does not have a versioning behavior")
	}
	if aw.pluginRegistryOptions.OnRegisterWorkflow != nil {
		aw.pluginRegistryOptions.OnRegisterWorkflow(w, options)
	}
	aw.registry.RegisterWorkflowWithOptions(w, options)
}

// RegisterDynamicWorkflow registers dynamic workflow implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterDynamicWorkflow(w interface{}, options DynamicRegisterWorkflowOptions) {
	if aw.workflowWorker == nil {
		panic("workflow worker disabled, cannot register workflow")
	}
	if options.LoadDynamicRuntimeOptions == nil && aw.executionParams.UseBuildIDForVersioning &&
		(aw.executionParams.DeploymentOptions.Version != WorkerDeploymentVersion{}) &&
		aw.executionParams.DeploymentOptions.DefaultVersioningBehavior == VersioningBehaviorUnspecified {
		panic("dynamic workflow does not have a versioning behavior")
	}
	if aw.pluginRegistryOptions.OnRegisterDynamicWorkflow != nil {
		aw.pluginRegistryOptions.OnRegisterDynamicWorkflow(w, DynamicRegisterWorkflowOptions{})
	}
	aw.registry.RegisterDynamicWorkflow(w, options)
}

// RegisterActivity registers activity implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterActivity(a interface{}) {
	if aw.pluginRegistryOptions.OnRegisterActivity != nil {
		aw.pluginRegistryOptions.OnRegisterActivity(a, RegisterActivityOptions{})
	}
	aw.registry.RegisterActivity(a)
}

// RegisterActivityWithOptions registers activity implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterActivityWithOptions(a interface{}, options RegisterActivityOptions) {
	if aw.pluginRegistryOptions.OnRegisterActivity != nil {
		aw.pluginRegistryOptions.OnRegisterActivity(a, options)
	}
	aw.registry.RegisterActivityWithOptions(a, options)
}

// RegisterDynamicActivity registers the dynamic activity function with options.
// Registering activities via a structure is not supported for dynamic activities.
func (aw *AggregatedWorker) RegisterDynamicActivity(a interface{}, options DynamicRegisterActivityOptions) {
	if aw.pluginRegistryOptions.OnRegisterActivity != nil {
		aw.pluginRegistryOptions.OnRegisterDynamicActivity(a, options)
	}
	aw.registry.RegisterDynamicActivity(a, options)
}

func (aw *AggregatedWorker) RegisterNexusService(service *nexus.Service) {
	if aw.started.Load() {
		panic(errors.New("cannot register Nexus services after worker start"))
	}
	if aw.pluginRegistryOptions.OnRegisterNexusService != nil {
		aw.pluginRegistryOptions.OnRegisterNexusService(service)
	}
	aw.registry.RegisterNexusService(service)
}

// Start the worker in a non-blocking fashion.
// The actual work is done in the memoized "start" function to ensure duplicate calls are returned a consistent error.
func (aw *AggregatedWorker) Start() error {
	aw.assertNotStopped()
	return aw.memoizedStart()
}

// start 是 Worker 真正的启动逻辑，被 memoizedStart 包裹以确保只执行一次。
//
// 启动流程（按顺序）：
//  1. 初始化二进制校验和 + 确保 Client 连接到 Server
//  2. 从 Server 拉取 Capabilities 和 Namespace 数据（payload 限制等）
//  3. 按顺序启动各子 Worker：workflowWorker → activityWorker → sessionWorker → nexusWorker
//     - 如果某个子 Worker 启动失败，会回滚已启动的子 Worker（保证优雅清理）
//  4. 注册心跳 Worker（如果启用了心跳）
//
// 注意：此方法通过 sync.OnceValue 被 memoized，所以是幂等的。
func (aw *AggregatedWorker) start() error {
	aw.started.Store(true)

	// --- 步骤 1：初始化校验和 + 确保 Client 连接就绪 ---
	// initBinaryChecksum 计算当前可执行文件的校验和，用于日志/调试时识别 Worker 版本
	if err := initBinaryChecksum(); err != nil {
		return fmt.Errorf("failed to get executable checksum: %v", err)
	} else if err = aw.client.ensureInitialized(context.Background()); err != nil {
		return err // ensureInitialized 确保 gRPC 连接、namespace 校验等已完成
	}

	// --- 步骤 2a：从 Server 拉取 Capabilities ---
	// 这是 &capabilities 指针唯一一次被写入的地方。各子 Worker 持有同一指针，
	// 它们会在运行时通过解引用来读取 Server 支持的特性。
	capabilities, err := aw.client.loadCapabilities(context.Background())
	if err != nil {
		return err
	}
	proto.Merge(aw.capabilities, capabilities)

	// --- 步骤 2b：从 Server 拉取 Namespace 数据（payload 大小限制等）---
	nsData, err := aw.client.loadNamespaceData(aw.executionParams.MetricsHandler)
	if err != nil {
		return err
	}

	// --- 步骤 2c：应用 Payload 错误限制 ---
	// setErrorLimits 是在 NewAggregatedWorker 阶段通过闭包注入的回调，
	// 它使用 Server 返回的 namespace 级别限制来配置 payload 大小检查逻辑。
	if aw.executionParams.setErrorLimits != nil {
		payloadSizeError := int64(0)
		if nsData.limits.BlobSizeLimitError > 0 {
			payloadSizeError = nsData.limits.BlobSizeLimitError
		}
		memoSizeError := int64(0)
		if nsData.limits.MemoSizeLimitError > 0 {
			memoSizeError = nsData.limits.MemoSizeLimitError
		}
		aw.executionParams.setErrorLimits(&payloadLimits{
			payloadSize: payloadSizeError,
			memoSize:    memoSizeError,
		})
	}

	// --- 步骤 2d：根据 Server Capabilities 设置运行时标志 ---
	// WorkerPollCompleteOnShutdown：Server 是否支持在关闭时等待当前 Poll 完成
	if nsData.capabilities.GetWorkerPollCompleteOnShutdown() {
		aw.workerPollCompleteOnShutdown.Store(true)
	}

	// PollerAutoscaling：Server 是否支持 Poller 自动扩缩容
	if nsData.capabilities.GetPollerAutoscaling() {
		aw.executionParams.serverSupportsAutoscaling.Store(true)
	}

	// --- 步骤 3a：启动 Workflow Worker ---
	// 同时将其注册到 Eager Dispatcher（如果 Client 支持 Eager Activity 分发）
	if !util.IsInterfaceNil(aw.workflowWorker) {
		if err := aw.workflowWorker.Start(); err != nil {
			return err
		}
		if aw.client.eagerDispatcher != nil {
			aw.client.eagerDispatcher.registerWorker(aw.workflowWorker)
		}
	}
	// --- 步骤 3b：启动 Activity Worker ---
	// 失败时会回滚：停止 workflowWorker 并 deregister Eager Dispatcher
	if !util.IsInterfaceNil(aw.activityWorker) {
		if err := aw.activityWorker.Start(); err != nil {
			// 回滚：停止 workflowWorker
			if !util.IsInterfaceNil(aw.workflowWorker) {
				if aw.workflowWorker.worker.isWorkerStarted {
					if aw.client.eagerDispatcher != nil {
						aw.client.eagerDispatcher.deregisterWorker(aw.workflowWorker)
					}
					aw.workflowWorker.Stop()
				}
			}
			return err
		}
	}

	// --- 步骤 3c：启动 Session Worker ---
	// 条件：Session 功能已启用 且 有已注册的 Activity。
	// 失败时回滚 workflowWorker + activityWorker。
	if !util.IsInterfaceNil(aw.sessionWorker) && len(aw.registry.getRegisteredActivities()) > 0 {
		aw.logger.Info("Starting session worker")
		if err := aw.sessionWorker.Start(); err != nil {
			// 回滚：停止 workflowWorker 和 activityWorker
			if !util.IsInterfaceNil(aw.workflowWorker) {
				if aw.workflowWorker.worker.isWorkerStarted {
					aw.workflowWorker.Stop()
				}
			}
			if !util.IsInterfaceNil(aw.activityWorker) {
				if aw.activityWorker.worker.isWorkerStarted {
					aw.activityWorker.Stop()
				}
			}
			return err
		}
	}
	// --- 步骤 3d：按需创建并启动 Nexus Worker ---
	// Nexus Worker 是唯一在 start() 中延迟创建的子 Worker。
	// 原因：Nexus Worker 的创建依赖于用户是否注册了 Nexus Service，
	// 而注册动作通常发生在 NewAggregatedWorker 和 Start() 之间。
	nexusServices := aw.registry.getRegisteredNexusServices()
	if len(nexusServices) > 0 {
		reg := nexus.NewServiceRegistry()
		for _, service := range nexusServices {
			if err := reg.Register(service); err != nil {
				return fmt.Errorf("failed to create a nexus worker: %w", err)
			}
		}
		reg.Use(nexusMiddleware(aw.registry.interceptors))
		handler, err := reg.NewHandler()
		if err != nil {
			return fmt.Errorf("failed to create a nexus worker: %w", err)
		}
		aw.nexusWorker, err = newNexusWorker(nexusWorkerOptions{
			executionParameters: aw.executionParams,
			client:              aw.client,
			workflowService:     aw.client.workflowService,
			handler:             handler,
			registry:            aw.registry,
		})
		if err != nil {
			return fmt.Errorf("failed to create a nexus worker: %w", err)
		}
		if err := aw.nexusWorker.Start(); err != nil {
			return fmt.Errorf("failed to start a nexus worker: %w", err)
		}
	}

	// --- 步骤 4：注册心跳 Worker ---
	// 启动一个独立的 goroutine，按 workerHeartbeatInterval 定时调用 heartbeatCallback
	// 构造心跳消息并上报给 Server。
	if aw.client.workerHeartbeatInterval > 0 {
		if err := aw.registerHeartbeatWorker(); err != nil {
			return fmt.Errorf("failed to register heartbeat worker: %w", err)
		}
	}
	aw.logger.Info("Started Worker")
	return nil
}

func (aw *AggregatedWorker) assertNotStopped() {
	stopped := true
	select {
	case <-aw.stopC:
	default:
		stopped = false
	}
	if stopped {
		panic("attempted to start a worker that has been stopped before")
	}
}

var (
	binaryChecksum     string
	binaryChecksumLock sync.Mutex
)

// SetBinaryChecksum sets the identifier of the binary(aka BinaryChecksum).
// The identifier is mainly used in recording reset points when respondWorkflowTaskCompleted. For each workflow, the very first
// workflow task completed by a binary will be associated as a auto-reset point for the binary. So that when a customer wants to
// mark the binary as bad, the workflow will be reset to that point -- which means workflow will forget all progress generated
// by the binary.
// On another hand, once the binary is marked as bad, the bad binary cannot poll workflow queue and make any progress any more.
func SetBinaryChecksum(checksum string) {
	binaryChecksumLock.Lock()
	defer binaryChecksumLock.Unlock()

	binaryChecksum = checksum
}

func initBinaryChecksum() error {
	binaryChecksumLock.Lock()
	defer binaryChecksumLock.Unlock()

	return initBinaryChecksumLocked()
}

func getBinaryChecksum() string {
	binaryChecksumLock.Lock()
	defer binaryChecksumLock.Unlock()

	if len(binaryChecksum) == 0 {
		err := initBinaryChecksumLocked()
		if err != nil {
			panic(err)
		}
	}

	return binaryChecksum
}

// Run the worker in a blocking fashion. Stop the worker when interruptCh receives signal.
// Pass worker.InterruptCh() to stop the worker with SIGINT or SIGTERM.
// Pass nil to stop the worker with external Stop() call.
// Pass any other `<-chan interface{}` and Run will wait for signal from that channel.
// Returns error if the worker fails to start or there is a fatal error
// during execution.
func (aw *AggregatedWorker) Run(interruptCh <-chan interface{}) error {
	if err := aw.Start(); err != nil {
		return err
	}
	select {
	case s := <-interruptCh:
		aw.logger.Info("Worker has been stopped.", "Signal", s)
		aw.Stop()
	case <-aw.stopC:
		aw.fatalErrLock.Lock()
		defer aw.fatalErrLock.Unlock()
		// This may be nil if this wasn't stopped due to fatal error
		return aw.fatalErr
	}
	return nil
}

// Stop the worker.
func (aw *AggregatedWorker) Stop() {
	// Only attempt stop if we haven't attempted before
	select {
	case <-aw.stopC:
		return
	default:
	}

	// Prevent pollers from re-polling before closing stopC. There is a race
	// between stopC being closed and the ShutdownWorker RPC: a poll can
	// complete naturally (e.g. long-poll timeout) right after stopC fires
	// but before ShutdownWorker is sent, causing the poller to loop and
	// re-poll.
	if !util.IsInterfaceNil(aw.activityWorker) {
		aw.activityWorker.worker.stopPolling()
	}
	if !util.IsInterfaceNil(aw.workflowWorker) {
		aw.workflowWorker.worker.stopPolling()
	}
	if !util.IsInterfaceNil(aw.nexusWorker) {
		aw.nexusWorker.worker.stopPolling()
	}
	if !util.IsInterfaceNil(aw.sessionWorker) {
		aw.sessionWorker.stopPolling()
	}

	close(aw.stopC)

	aw.sendShutdownWorkerRPC()

	// Issue stop through plugins
	stop := func(context.Context, WorkerPluginStopWorkerOptions) {
		if !util.IsInterfaceNil(aw.workflowWorker) {
			if aw.client.eagerDispatcher != nil {
				aw.client.eagerDispatcher.deregisterWorker(aw.workflowWorker)
			}
			aw.workflowWorker.Stop()
		}
		if !util.IsInterfaceNil(aw.activityWorker) {
			aw.activityWorker.Stop()
		}
		if !util.IsInterfaceNil(aw.sessionWorker) {
			aw.sessionWorker.Stop()
		}
		if !util.IsInterfaceNil(aw.nexusWorker) {
			aw.nexusWorker.Stop()
		}
	}
	for i := len(aw.plugins) - 1; i >= 0; i-- {
		plugin := aw.plugins[i]
		next := stop
		stop = func(ctx context.Context, options WorkerPluginStopWorkerOptions) {
			plugin.StopWorker(ctx, options, next)
		}
	}
	stop(context.Background(), WorkerPluginStopWorkerOptions{
		WorkerInstanceKey: aw.workerInstanceKey,
	})

	aw.unregisterHeartbeatWorker()

	aw.logger.Info("Stopped Worker")
}

func (aw *AggregatedWorker) registerHeartbeatWorker() error {
	if aw.client.heartbeatManager == nil {
		return nil
	}
	return aw.client.heartbeatManager.registerWorker(aw)
}

func (aw *AggregatedWorker) unregisterHeartbeatWorker() {
	if aw.client.heartbeatManager == nil {
		return
	}
	aw.client.heartbeatManager.unregisterWorker(aw)
}

// sendShutdownWorkerRPC sends a ShutdownWorker RPC to notify the server that this worker is shutting down.
// When StickyTaskQueue is non-empty, this is a best-effort attempt to indicate to Matching service
// that this workflow task poller's sticky queue will no longer be polled.
//
// NOTE: errors are logged but don't fail the shutdown.
func (aw *AggregatedWorker) sendShutdownWorkerRPC() {
	aw.shuttingDown.Store(true)

	ctx := context.Background()
	grpcCtx, cancel := newGRPCContext(ctx, grpcMetricsHandler(aw.executionParams.MetricsHandler))
	defer cancel()

	var heartbeat *workerpb.WorkerHeartbeat
	if aw.heartbeatCallback != nil {
		heartbeat = aw.heartbeatCallback()
	}

	var stickyTaskQueue string
	if aw.workflowWorker != nil && aw.workflowWorker.stickyUUID != "" {
		stickyTaskQueue = getWorkerTaskQueue(aw.workflowWorker.stickyUUID)
	}

	aw.sendShutdownWorkerRPCForTaskQueue(grpcCtx, aw.executionParams.TaskQueue, stickyTaskQueue, aw.activeTaskQueueTypes(), heartbeat)

	if util.IsInterfaceNil(aw.sessionWorker) {
		return
	}

	aw.sendShutdownWorkerRPCForTaskQueue(
		grpcCtx,
		aw.sessionWorker.getCreationWorkerTaskQueue(),
		"",
		[]enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		nil,
	)
	aw.sendShutdownWorkerRPCForTaskQueue(
		grpcCtx,
		aw.sessionWorker.getActivityWorkerTaskQueue(),
		"",
		[]enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		nil,
	)
}

func (aw *AggregatedWorker) sendShutdownWorkerRPCForTaskQueue(
	grpcCtx context.Context,
	taskQueue string,
	stickyTaskQueue string,
	taskQueueTypes []enumspb.TaskQueueType,
	heartbeat *workerpb.WorkerHeartbeat,
) {
	_, err := aw.client.workflowService.ShutdownWorker(grpcCtx, &workflowservice.ShutdownWorkerRequest{
		Namespace:         aw.executionParams.Namespace,
		StickyTaskQueue:   stickyTaskQueue,
		Identity:          aw.executionParams.Identity,
		Reason:            "graceful shutdown",
		WorkerHeartbeat:   heartbeat,
		WorkerInstanceKey: aw.workerInstanceKey,
		TaskQueue:         taskQueue,
		TaskQueueTypes:    taskQueueTypes,
	})

	// Ignore unimplemented (server doesn't support it)
	if _, isUnimplemented := err.(*serviceerror.Unimplemented); isUnimplemented {
		return
	}

	if err != nil {
		aw.logger.Warn("ShutdownWorker rpc errored during worker shutdown.", tagError, err)
	}
}

func (aw *AggregatedWorker) activeTaskQueueTypes() []enumspb.TaskQueueType {
	var types []enumspb.TaskQueueType
	if !util.IsInterfaceNil(aw.workflowWorker) {
		types = append(types, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	}
	if !util.IsInterfaceNil(aw.activityWorker) {
		types = append(types, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	}
	if !util.IsInterfaceNil(aw.nexusWorker) {
		types = append(types, enumspb.TASK_QUEUE_TYPE_NEXUS)
	}
	return types
}

// WorkflowReplayer is used to replay workflow code from an event history
type WorkflowReplayer struct {
	registry                    *registry
	dataConverter               converter.DataConverter
	failureConverter            converter.FailureConverter
	contextPropagators          []ContextPropagator
	enableLoggingInReplay       bool
	disableDeadlockDetection    bool
	inboundPayloadVisitor       PayloadVisitor
	mu                          sync.Mutex
	workflowExecutionResults    map[string]*commonpb.Payloads
	workflowReplayerInstanceKey string
	plugins                     []WorkerPlugin
	pluginRegistryOptions       *WorkerPluginConfigureWorkflowReplayerRegistryOptions
}

// WorkflowReplayerOptions are options for creating a workflow replayer.
type WorkflowReplayerOptions struct {
	// Optional custom data converter to provide for replay. If not set, the
	// default converter is used.
	DataConverter converter.DataConverter

	FailureConverter converter.FailureConverter

	// Optional: Sets ContextPropagators that allows users to control the context information passed through a workflow
	//
	// default: nil
	ContextPropagators []ContextPropagator

	// Interceptors to apply to the worker. Earlier interceptors wrap later
	// interceptors.
	Interceptors []WorkerInterceptor

	// Disable aliasing during registration. This should be set if it was set on
	// worker.Options.DisableRegistrationAliasing when originally run. See
	// documentation for that field for more information.
	DisableRegistrationAliasing bool

	// Optional: Enable logging in replay.
	// In the workflow code you can use workflow.GetLogger(ctx) to write logs. By default, the logger will skip log
	// entry during replay mode so you won't see duplicate logs. This option will enable the logging in replay mode.
	// This is only useful for debugging purpose.
	//
	// default: false
	EnableLoggingInReplay bool

	// Optional: Disable the default 1 second deadlock detection timeout. This option can be used to step through
	// workflow code with multiple breakpoints in a debugger.
	DisableDeadlockDetection bool

	// Plugins that can configure options and intercept replays.
	//
	// Plugins themselves should never mutate this field, the behavior is
	// undefined.
	//
	// NOTE: Experimental
	Plugins []WorkerPlugin

	// ExternalStorage configures external payload storage for replay.
	// Set this to the same ExternalStorage used by the original worker so that
	// externally stored payloads in the history are resolved before being
	// passed to the workflow code.
	//
	// NOTE: Experimental
	ExternalStorage converter.ExternalStorage
}

// ReplayWorkflowHistoryOptions are options for replaying a workflow.
type ReplayWorkflowHistoryOptions struct {
	// OriginalExecution - Overide the workflow execution details used for replay.
	// Optional
	OriginalExecution WorkflowExecution
}

// NewWorkflowReplayer creates an instance of the WorkflowReplayer.
func NewWorkflowReplayer(options WorkflowReplayerOptions) (*WorkflowReplayer, error) {
	// Configure replayer
	workflowReplayerInstanceKey := uuid.NewString()
	var pluginRegistryOptions WorkerPluginConfigureWorkflowReplayerRegistryOptions
	for _, plugin := range options.Plugins {
		if err := plugin.ConfigureWorkflowReplayer(context.Background(), WorkerPluginConfigureWorkflowReplayerOptions{
			WorkflowReplayerInstanceKey:     workflowReplayerInstanceKey,
			WorkflowReplayerOptions:         &options,
			WorkflowReplayerRegistryOptions: &pluginRegistryOptions,
		}); err != nil {
			return nil, err
		}
	}

	storageParams, err := extstore.ExternalStorageToParams(options.ExternalStorage)
	if err != nil {
		return nil, fmt.Errorf("invalid ExternalStorage options: %w", err)
	}

	registry := newRegistryWithOptions(registryOptions{disableAliasing: options.DisableRegistrationAliasing})
	registry.interceptors = options.Interceptors
	return &WorkflowReplayer{
		registry:                    registry,
		dataConverter:               options.DataConverter,
		failureConverter:            options.FailureConverter,
		contextPropagators:          options.ContextPropagators,
		enableLoggingInReplay:       options.EnableLoggingInReplay,
		disableDeadlockDetection:    options.DisableDeadlockDetection,
		inboundPayloadVisitor:       extstore.NewExternalRetrievalVisitor(storageParams),
		workflowExecutionResults:    make(map[string]*commonpb.Payloads),
		workflowReplayerInstanceKey: workflowReplayerInstanceKey,
		plugins:                     options.Plugins,
		pluginRegistryOptions:       &pluginRegistryOptions,
	}, nil
}

// RegisterWorkflow registers workflow function to replay
func (aw *WorkflowReplayer) RegisterWorkflow(w interface{}) {
	if aw.pluginRegistryOptions.OnRegisterWorkflow != nil {
		aw.pluginRegistryOptions.OnRegisterWorkflow(w, RegisterWorkflowOptions{})
	}
	aw.registry.RegisterWorkflow(w)
}

// RegisterWorkflowWithOptions registers workflow function with custom workflow name to replay
func (aw *WorkflowReplayer) RegisterWorkflowWithOptions(w interface{}, options RegisterWorkflowOptions) {
	if aw.pluginRegistryOptions.OnRegisterWorkflow != nil {
		aw.pluginRegistryOptions.OnRegisterWorkflow(w, options)
	}
	aw.registry.RegisterWorkflowWithOptions(w, options)
}

// RegisterDynamicWorkflow registers a dynamic workflow function to replay
func (aw *WorkflowReplayer) RegisterDynamicWorkflow(w interface{}, options DynamicRegisterWorkflowOptions) {
	if aw.pluginRegistryOptions.OnRegisterDynamicWorkflow != nil {
		aw.pluginRegistryOptions.OnRegisterDynamicWorkflow(w, options)
	}
	aw.registry.RegisterDynamicWorkflow(w, options)
}

// ReplayWorkflowHistoryWithOptions executes a single workflow task for the given history.
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayWorkflowHistoryWithOptions(logger log.Logger, history *historypb.History, options ReplayWorkflowHistoryOptions) error {
	if logger == nil {
		logger = ilog.NewDefaultLogger()
	}

	controller := gomock.NewController(ilog.NewTestReporter(logger))
	service := workflowservicemock.NewMockWorkflowServiceClient(controller)

	return aw.replayWorkflowHistory(logger, service, ReplayNamespace, options.OriginalExecution, history)
}

// ReplayWorkflowHistory executes a single workflow task for the given history.
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayWorkflowHistory(logger log.Logger, history *historypb.History) error {
	return aw.ReplayWorkflowHistoryWithOptions(logger, history, ReplayWorkflowHistoryOptions{})
}

// ReplayWorkflowHistoryFromJSONFile executes a single workflow task for the given json history file.
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayWorkflowHistoryFromJSONFile(logger log.Logger, jsonfileName string) error {
	return aw.ReplayPartialWorkflowHistoryFromJSONFile(logger, jsonfileName, 0)
}

// ReplayPartialWorkflowHistoryFromJSONFile executes a single workflow task for the given json history file upto provided
// lastEventID(inclusive).
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayPartialWorkflowHistoryFromJSONFile(logger log.Logger, jsonfileName string, lastEventID int64) error {
	history, err := extractHistoryFromFile(jsonfileName, lastEventID)
	if err != nil {
		return err
	}

	if logger == nil {
		logger = ilog.NewDefaultLogger()
	}

	controller := gomock.NewController(ilog.NewTestReporter(logger))
	service := workflowservicemock.NewMockWorkflowServiceClient(controller)

	return aw.replayWorkflowHistory(logger, service, ReplayNamespace, WorkflowExecution{}, history)
}

// ReplayWorkflowExecution replays workflow execution loading it from Temporal service.
func (aw *WorkflowReplayer) ReplayWorkflowExecution(ctx context.Context, service workflowservice.WorkflowServiceClient, logger log.Logger, namespace string, execution WorkflowExecution) error {
	if logger == nil {
		logger = ilog.NewDefaultLogger()
	}

	sharedExecution := &commonpb.WorkflowExecution{
		RunId:      execution.RunID,
		WorkflowId: execution.ID,
	}
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: sharedExecution,
	}
	var history historypb.History
	for {
		resp, err := service.GetWorkflowExecutionHistory(ctx, request)
		if err != nil {
			return err
		}
		currHistory := resp.History
		if resp.RawHistory != nil {
			currHistory, err = serializer.DeserializeBlobDataToHistoryEvents(resp.RawHistory, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			if err != nil {
				return err
			}
		}
		if currHistory == nil {
			break
		}
		history.Events = append(history.Events, currHistory.Events...)
		if len(resp.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = resp.NextPageToken
	}
	return aw.replayWorkflowHistory(logger, service, namespace, execution, &history)
}

// GetWorkflowResult get the result of a succesfully replayed workflow.
func (aw *WorkflowReplayer) GetWorkflowResult(workflowID string, valuePtr interface{}) error {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	if workflowID == "" {
		workflowID = "ReplayId"
	}
	payloads, ok := aw.workflowExecutionResults[workflowID]
	if !ok {
		return errors.New("workflow result not found")
	}
	dc := aw.dataConverter
	if dc == nil {
		dc = converter.GetDefaultDataConverter()
	}
	return dc.FromPayloads(payloads, valuePtr)
}

func (aw *WorkflowReplayer) replayWorkflowHistory(
	logger log.Logger,
	service workflowservice.WorkflowServiceClient,
	namespace string,
	originalExecution WorkflowExecution,
	history *historypb.History,
) error {
	replay := func(ctx context.Context, options WorkerPluginReplayWorkflowOptions) error {
		return aw.replayWorkflowHistoryRoot(
			options.Logger,
			options.WorkflowServiceClient,
			options.Namespace,
			options.OriginalExecution,
			options.History,
		)
	}
	for i := len(aw.plugins) - 1; i >= 0; i-- {
		plugin := aw.plugins[i]
		next := replay
		replay = func(ctx context.Context, options WorkerPluginReplayWorkflowOptions) error {
			return plugin.ReplayWorkflow(ctx, options, next)
		}
	}
	return replay(context.Background(), WorkerPluginReplayWorkflowOptions{
		WorkflowReplayerInstanceKey: aw.workflowReplayerInstanceKey,
		History:                     history,
		Logger:                      logger,
		WorkflowServiceClient:       service,
		Namespace:                   namespace,
		OriginalExecution:           originalExecution,
		WorkflowReplayRegistry:      aw,
	})
}

func (aw *WorkflowReplayer) replayWorkflowHistoryRoot(
	logger log.Logger,
	service workflowservice.WorkflowServiceClient,
	namespace string,
	originalExecution WorkflowExecution,
	history *historypb.History,
) error {
	taskQueue := "ReplayTaskQueue"
	events := history.Events
	if events == nil {
		return errors.New("empty events")
	}
	if len(events) < 3 {
		return errors.New("at least 3 events expected in the history")
	}
	first := events[0]
	if first.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		return errors.New("first event is not WorkflowExecutionStarted")
	}
	last := events[len(events)-1]

	attr := first.GetWorkflowExecutionStartedEventAttributes()
	if attr == nil {
		return errors.New("corrupted WorkflowExecutionStarted")
	}
	workflowType := attr.WorkflowType
	execution := &commonpb.WorkflowExecution{
		RunId:      uuid.NewString(),
		WorkflowId: "ReplayId",
	}
	if originalExecution.ID != "" {
		execution.WorkflowId = originalExecution.ID
	}
	if originalExecution.RunID != "" {
		execution.RunId = originalExecution.RunID
	} else if first.GetWorkflowExecutionStartedEventAttributes().GetOriginalExecutionRunId() != "" {
		execution.RunId = first.GetWorkflowExecutionStartedEventAttributes().GetOriginalExecutionRunId()
	}

	if first.GetWorkflowExecutionStartedEventAttributes().GetTaskQueue().GetName() != "" {
		taskQueue = first.GetWorkflowExecutionStartedEventAttributes().GetTaskQueue().GetName()
	}

	task := &workflowservice.PollWorkflowTaskQueueResponse{
		Attempt:                1,
		TaskToken:              []byte("ReplayTaskToken"),
		WorkflowType:           workflowType,
		WorkflowExecution:      execution,
		History:                history,
		PreviousStartedEventId: math.MaxInt64,
	}

	iterator := &retrievingHistoryIterator{
		inner: &historyIteratorImpl{
			nextPageToken: task.NextPageToken,
			execution:     task.WorkflowExecution,
			namespace:     ReplayNamespace,
			service:       service,
			taskQueue:     taskQueue,
		},
		inboundVisitor: aw.inboundPayloadVisitor,
	}
	cache := NewWorkerCache()
	params := workerExecutionParameters{
		Namespace:             namespace,
		TaskQueue:             taskQueue,
		Identity:              "replayID",
		Logger:                logger,
		cache:                 cache,
		DataConverter:         aw.dataConverter,
		FailureConverter:      aw.failureConverter,
		ContextPropagators:    aw.contextPropagators,
		EnableLoggingInReplay: aw.enableLoggingInReplay,
		// Hardcoding NopHandler avoids "No metrics handler configured for temporal worker"
		// logs during replay.
		MetricsHandler: metrics.NopHandler,
		capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{
			SignalAndQueryHeader:            true,
			InternalErrorDifferentiation:    true,
			ActivityFailureIncludeHeartbeat: true,
			SupportsSchedules:               true,
			EncodedFailureAttributes:        true,
			UpsertMemo:                      true,
			EagerWorkflowStart:              true,
			SdkMetadata:                     true,
		},
	}
	if aw.disableDeadlockDetection {
		params.DeadlockDetectionTimeout = math.MaxInt64
	}
	// Resolve externally stored payloads in the history before passing to the
	// task handler. This mirrors what processWorkflowTask does for live workers.
	if err := visitProtoPayloads(context.Background(), aw.inboundPayloadVisitor, task, 0); err != nil {
		return err
	}

	taskHandler := newWorkflowTaskHandler(params, nil, aw.registry)
	wfctx, err := taskHandler.GetOrCreateWorkflowContext(task, iterator)
	defer wfctx.Unlock(err)
	if err != nil {
		return err
	}
	resp, err := taskHandler.ProcessWorkflowTask(&workflowTask{task: task, historyIterator: iterator}, wfctx, nil)
	if err != nil {
		return err
	}

	if resp != nil {
		if failedReq, ok := resp.rawRequest.(*workflowservice.RespondWorkflowTaskFailedRequest); ok {
			return fmt.Errorf("replay workflow failed with failure: %v", failedReq.GetFailure())
		}
	}

	if last.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED && last.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
		return nil
	}

	var rawRequest proto.Message
	if resp != nil {
		completeReq, ok := resp.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
		if ok {
			for _, d := range completeReq.Commands {
				if d.GetCommandType() == enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION {
					if last.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
						return nil
					}
				}
				if d.GetCommandType() == enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION {
					if last.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
						aw.mu.Lock()
						defer aw.mu.Unlock()
						aw.workflowExecutionResults[execution.WorkflowId] = d.GetCompleteWorkflowExecutionCommandAttributes().Result
						return nil
					}
				}
			}
		}
		rawRequest = resp.rawRequest
	}
	return fmt.Errorf("replay workflow doesn't return the same result as the last event, resp: %[1]T{%[1]v}, last: %[2]T{%[2]v}", rawRequest, last)
}

// HistoryFromJSON deserializes history from a reader of JSON bytes. This does
// not close the reader if it is closeable.
func HistoryFromJSON(r io.Reader, lastEventID int64) (*historypb.History, error) {
	// We set DiscardUnknown here because the history may have been created by a previous
	// version of our protos
	opts := temporalproto.CustomJSONUnmarshalOptions{
		DiscardUnknown: true,
	}
	bs, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	hist := &historypb.History{}
	if err := opts.Unmarshal(bs, hist); err != nil {
		return nil, err
	}

	// If there is a last event ID, slice the rest off
	if lastEventID > 0 {
		for i, event := range hist.Events {
			if event.EventId == lastEventID {
				// Inclusive
				hist.Events = hist.Events[:i+1]
				break
			}
		}
	}
	return hist, nil
}

func extractHistoryFromFile(jsonfileName string, lastEventID int64) (hist *historypb.History, err error) {
	reader, err := os.Open(jsonfileName)
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := reader.Close()
		if closeErr != nil && err == nil {
			err = closeErr
		} else if closeErr != nil {
			ilog.NewDefaultLogger().Warn("failed to close json file", "path", jsonfileName, "error", closeErr)
		}
	}()

	opts := temporalproto.CustomJSONUnmarshalOptions{
		DiscardUnknown: true,
	}

	bs, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	hist = &historypb.History{}
	if err := opts.Unmarshal(bs, hist); err != nil {
		return nil, err
	}

	// If there is a last event ID, slice the rest off
	if lastEventID > 0 {
		for i, event := range hist.Events {
			if event.EventId == lastEventID {
				// Inclusive
				hist.Events = hist.Events[:i+1]
				break
			}
		}
	}

	return hist, err
}

// NewAggregatedWorker returns an instance to manage both activity and workflow workers
// NewAggregatedWorker 创建一个聚合 Worker 实例，它是 Temporal Go SDK 中 Worker 的核心入口。
//
// 生命周期概览：
//  1. NewAggregatedWorker(): 构造 + 校验参数 + 创建各子 Worker + 设置心跳回调 + 包装 memoizedStart
//  2. RegisterWorkflow/RegisterActivity/...: 注册 Workflow/Activity/Nexus Service
//  3. Start() -> memoizedStart() -> start(): 连接 Server、获取 capabilities、启动各子 Worker、启动心跳
//  4. Stop(): 逐个关闭子 Worker、deregister 心跳、等待完成
//
// 参数：
//   - client: SDK 客户端，包含 gRPC 连接、DataConverter、interceptors 等基础能力
//   - taskQueue: Worker 监听的任务队列名称（不能是 Temporal 内部队列）
//   - options: Worker 的行为配置（并发数、版本控制、Session、插件等）
//
// 返回值总不为 nil；参数校验错误会直接 panic（属于编程错误，应尽早暴露）。
func NewAggregatedWorker(client *WorkflowClient, taskQueue string, options WorkerOptions) *AggregatedWorker {
	// =========================================================================
	// 阶段 1：基础参数校验
	// =========================================================================

	// Temporal 内部保留的 TaskQueue 前缀（如 "/_sys/"），SDK Worker 不允许使用
	if strings.HasPrefix(taskQueue, temporalPrefix) {
		panic(temporalPrefixError)
	}

	// =========================================================================
	// 阶段 2：插件（Plugin）初始化
	// =========================================================================
	// 合并 Client 级别和 Worker 级别的插件，然后依次调用 ConfigureWorker 生命周期钩子。
	// 插件列表顺序：先 Client 插件，后 Worker 插件。
	// 注意：这里使用了三参数 append 技巧 (append([]T(nil), src...)) 来创建一个新的 slice，
	// 避免对 client.workerPlugins 的后续修改影响到本 Worker。
	workerInstanceKey := uuid.NewString()
	var pluginRegistryOptions WorkerPluginConfigureWorkerRegistryOptions
	plugins := append(append([]WorkerPlugin(nil), client.workerPlugins...), options.Plugins...)
	for _, plugin := range plugins {
		// ConfigureWorker 允许插件修改 WorkerOptions 和 WorkerRegistryOptions，
		// 所以需要在 apply defaults 之前执行。此时没有有意义的 context，且所有 error 都是
		// 代码级别的配置错误，直接 panic 是合适的。
		if err := plugin.ConfigureWorker(context.Background(), WorkerPluginConfigureWorkerOptions{
			WorkerInstanceKey:     workerInstanceKey,
			TaskQueue:             taskQueue,
			WorkerOptions:         &options,
			WorkerRegistryOptions: &pluginRegistryOptions,
		}); err != nil {
			panic(err)
		}
	}

	// =========================================================================
	// 阶段 3：填充默认值 + 参数二次校验
	// =========================================================================
	setClientDefaults(client)
	setWorkerOptionsDefaults(&options)
	ctx := options.BackgroundActivityContext
	if ctx == nil {
		ctx = context.Background()
	}
	// backgroundActivityContext 是所有 Activity 执行的根 Context。
	// 当 Worker 停止时，通过 backgroundActivityContextCancel 取消该 Context，
	// 从而实现对所有正在执行的 Activity 的级联取消。
	backgroundActivityContext, backgroundActivityContextCancel := context.WithCancelCause(ctx)

	// 为什么 MaxConcurrentWorkflowTaskPollers 不允许为 1？
	// Sticky Queue（黏性队列）会占用 1 个 poller。如果总 poller 数只有 1，
	// 那这个 poller 永远挂在 sticky queue 上，普通 queue 永远得不到服务，
	// 导致此 Worker 无法接收新的 Workflow Task（首次匹配或 sticky 失效后的 fallback）。
	if options.MaxConcurrentWorkflowTaskPollers == 1 {
		panic("cannot set MaxConcurrentWorkflowTaskPollers to 1")
	}

	// 为什么 MaxConcurrentWorkflowTaskExecutionSize 不允许为 1？
	// 运行中的 poller 数量受 MaxConcurrentWorkflowTaskExecutionSize 限制。
	// 如果该值为 1，那仅有的一个 poller 也是永远挂在 sticky queue 上，
	// 同样无法接收普通队列的任务。
	if options.MaxConcurrentWorkflowTaskExecutionSize == 1 {
		panic("cannot set MaxConcurrentWorkflowTaskExecutionSize to 1")
	}

	// Session Worker 与 Worker Versioning 互不兼容
	// 参见：https://github.com/temporalio/sdk-go/issues/1227
	if options.EnableSessionWorker && options.UseBuildIDForVersioning {
		panic("cannot set both EnableSessionWorker and UseBuildIDForVersioning")
	}

	// 如果 DeploymentOptions 中指定了 Version，将其 BuildID 同步到此 Worker 的 BuildID
	if (options.DeploymentOptions.Version != WorkerDeploymentVersion{}) {
		options.BuildID = options.DeploymentOptions.Version.BuildID
	}
	// 语义冲突校验：如果未启用版本控制，却设置了默认的版本行为，这没有意义
	if !options.DeploymentOptions.UseVersioning &&
		options.DeploymentOptions.DefaultVersioningBehavior != VersioningBehaviorUnspecified {
		panic("cannot set both DeploymentOptions.DefaultVersioningBehavior if DeploymentOptions.UseBuildIDForVersioning is false")
	}

	if options.MaxConcurrentWorkflowTaskExternalStorageVisits < 0 {
		panic("MaxConcurrentWorkflowTaskExternalStorageVisits must not be negative")
	}

	// =========================================================================
	// 阶段 4：构造致命错误回调（fatalErrorCallback）
	// =========================================================================
	// 致命错误回调的设计要点：
	//   1. 这是一个闭包，引用了尚未创建完成的 aw（AggregatedWorker），所以需要先用 var aw *AggregatedWorker 占位。
	//   2. 使用互斥锁保护 fatalErr，确保只有第一个致命错误被记录（幂等性）。
	//   3. 记录错误后依次：调用用户注册的 OnFatalError 回调 → 触发 Stop() 停止整个 Worker。
	//   4. select 判断 stopC 是否已关闭：如果已关闭说明 Worker 已经在停止中，无需重复 Stop()。
	var aw *AggregatedWorker
	fatalErrorCallback := func(err error) {
		aw.fatalErrLock.Lock()
		alreadySet := aw.fatalErr != nil
		if !alreadySet {
			aw.fatalErr = err // 只有第一个致命错误被记录
		}
		aw.fatalErrLock.Unlock()
		if !alreadySet {
			if options.OnFatalError != nil {
				options.OnFatalError(err) // 通知用户注册的错误处理回调
			}
			select {
			case <-aw.stopC:
				// stopC 已关闭 → Worker 已经在停止流程中，不需要再次调用 Stop()
			default:
				aw.Stop() // 触发 Worker 的优雅关闭
			}
		}
	}
	// capabilities 是一个"延迟填充"的值。
	// 在 NewAggregatedWorker 阶段，还没有与 Server 建立连接，无法获取 Server 的能力信息。
	// 这里只声明一个零值结构体，把 &capabilities 指针放入 workerExecutionParameters，
	// 各子 Worker 通过解引用这个指针来访问 capabilities。
	// 真正的填充发生在 start() -> loadCapabilities() 中（此时已连上 Server）。
	var capabilities workflowservice.GetSystemInfoResponse_Capabilities

	// =========================================================================
	// 阶段 5：Metrics（指标）、Identity、Logger 初始化
	// =========================================================================
	// 基础 Metrics Handler 会附加上 TaskQueue 标签，确保同一个进程内不同 TaskQueue
	// 的 Worker 指标可以区分。
	baseMetricsHandler := client.metricsHandler.WithTags(metrics.TaskQueueTags(taskQueue))
	var metricsHandler metrics.Handler
	var heartbeatMetrics *heartbeatMetricsHandler

	if client.workerHeartbeatInterval != 0 {
		// 如果启用了 Worker 心跳（workerHeartbeatInterval > 0），
		// 使用 heartbeatMetricsHandler 包装基础 Metrics Handler。
		// 这个包装器会在心跳构造时填充累计的执行计数和失败计数等指标。
		heartbeatMetrics = newHeartbeatMetricsHandler(baseMetricsHandler)
		metricsHandler = heartbeatMetrics
	} else {
		metricsHandler = baseMetricsHandler
	}

	// Worker 的身份标识：优先使用 options.Identity，否则使用 client 默认的 identity（通常是进程 PID）
	identity := client.identity
	if options.Identity != "" {
		identity = options.Identity
	}

	// 构造结构化 Logger，附加 namespace、taskQueue、workerID 三个标签，
	// 使得日志可以按这些维度检索和过滤。
	logger := client.logger
	if logger == nil {
		logger = ilog.NewDefaultLogger()
	}
	logger = log.With(logger,
		tagNamespace, client.namespace,
		tagTaskQueue, taskQueue,
		tagWorkerID, identity,
	)
	if options.BuildID != "" {
		// 如果用户指定了 Build ID（Worker 版本标识），也附加到日志中
		logger = log.With(logger,
			tagBuildID, options.BuildID,
		)
	}

	// =========================================================================
	// 阶段 6：Payload Visitor + Worker Cache + 组装 workerExecutionParameters
	// =========================================================================

	// payloadLimitVisitor 用于检查 Payload 是否超过 Server 限制，超限时打 WARN 日志
	payloadLimitVisitor, setErrorLimits := newPayloadLimitsVisitor(client.payloadWarningLimits, logger)

	// cache 是 Worker 级别的 LRU 缓存，用于 Workflow 的 History 重放等场景
	cache := NewWorkerCache()
	// workerPollCompleteOnShutdown：是否在 Shutdown 时等待当前 Poll 完成再退出
	workerPollCompleteOnShutdown := &atomic.Bool{}

	// workerExecutionParameters 是所有执行参数的聚合体，被各子 Worker 共享。
	// 它包含了从 client、options 和函数内部推导出来的所有运行时参数。
	workerParams := workerExecutionParameters{
		Namespace:                        client.namespace,
		TaskQueue:                        taskQueue,
		Tuner:                            options.Tuner,
		WorkerActivitiesPerSecond:        options.WorkerActivitiesPerSecond,
		WorkerLocalActivitiesPerSecond:   options.WorkerLocalActivitiesPerSecond,
		Identity:                         identity,
		WorkerBuildID:                    options.BuildID,
		UseBuildIDForVersioning:          options.UseBuildIDForVersioning || options.DeploymentOptions.UseVersioning,
		DeploymentOptions:                options.DeploymentOptions,
		MetricsHandler:                   metricsHandler,
		Logger:                           logger,
		EnableLoggingInReplay:            options.EnableLoggingInReplay,
		BackgroundContext:                backgroundActivityContext,
		BackgroundContextCancel:          backgroundActivityContextCancel,
		StickyScheduleToStartTimeout:     options.StickyScheduleToStartTimeout,
		TaskQueueActivitiesPerSecond:     options.TaskQueueActivitiesPerSecond,
		WorkflowPanicPolicy:              options.WorkflowPanicPolicy,
		DataConverter:                    client.dataConverter,
		FailureConverter:                 client.failureConverter,
		WorkerStopTimeout:                options.WorkerStopTimeout,
		WorkerFatalErrorCallback:         fatalErrorCallback,
		ContextPropagators:               client.contextPropagators,
		DeadlockDetectionTimeout:         options.DeadlockDetectionTimeout,
		DefaultHeartbeatThrottleInterval: options.DefaultHeartbeatThrottleInterval,
		MaxHeartbeatThrottleInterval:     options.MaxHeartbeatThrottleInterval,
		cache:                            cache,
		// eagerActivityExecutor 用于支持 Eager Activity（Activity 不经过 TaskQueue 直接分发给同一 Worker）
		eagerActivityExecutor: newEagerActivityExecutor(eagerActivityExecutorOptions{
			disabled:      options.DisableEagerActivities,
			taskQueue:     taskQueue,
			maxConcurrent: options.MaxConcurrentEagerActivityExecutionSize,
		}),
		capabilities:                 &capabilities,      // 延迟填充：start() 中从 Server 拉取
		pollTimeTracker:              &pollTimeTracker{}, // 追踪各 Poll 的耗时和空闲时间
		workerInstanceKey:            workerInstanceKey,
		workerPollCompleteOnShutdown: workerPollCompleteOnShutdown,
		serverSupportsAutoscaling:    &atomic.Bool{}, // 延迟填充：start() 中根据 Server 能力设置
		// inboundPayloadVisitor：处理进入 Worker 的 Payload（如外部存储引用解析）
		inboundPayloadVisitor: extstore.NewExternalRetrievalVisitor(client.storageParams),
		// outboundPayloadVisitor：处理离开 Worker 的 Payload（如外部存储上传 + Payload 大小限制检查）
		outboundPayloadVisitor: newCompositePayloadVisitor(
			extstore.NewExternalStorageVisitor(client.storageParams),
			payloadLimitVisitor,
		),
		payloadVisitorConcurrency: options.MaxConcurrentWorkflowTaskExternalStorageVisits,
		// setErrorLimits 闭包：在 start() 中从 Server 拉取到 namespace 级别限制后调用
		setErrorLimits: func(limits *payloadLimits) {
			if !options.DisablePayloadErrorLimit {
				setErrorLimits(limits)
			}
		},
	}

	// =========================================================================
	// 阶段 7：Poller 行为策略设置
	// =========================================================================
	// 每种任务类型（Workflow/Activity/Nexus）都支持两种 Poller 策略配置方式：
	//   a) 简单模式：设置 MaxConcurrentXxxTaskPollers（用内置的 PollerBehaviorSimpleMaximum）
	//   b) 高级模式：传入自定义 PollerBehavior（支持自动扩缩容等复杂策略）
	// 二者必须至少设置一个，否则 panic。同时设置了的话，简单模式优先。

	// Workflow Task Poller
	if options.MaxConcurrentWorkflowTaskPollers != 0 {
		workerParams.WorkflowTaskPollerBehavior = NewPollerBehaviorSimpleMaximum(PollerBehaviorSimpleMaximumOptions{
			MaximumNumberOfPollers: options.MaxConcurrentWorkflowTaskPollers,
		})
	} else if options.WorkflowTaskPollerBehavior != nil {
		workerParams.WorkflowTaskPollerBehavior = options.WorkflowTaskPollerBehavior
	} else {
		panic("must set either MaxConcurrentWorkflowTaskPollers or WorkflowTaskPollerBehavior")
	}

	// Activity Task Poller
	if options.MaxConcurrentActivityTaskPollers != 0 {
		workerParams.ActivityTaskPollerBehavior = NewPollerBehaviorSimpleMaximum(PollerBehaviorSimpleMaximumOptions{
			MaximumNumberOfPollers: options.MaxConcurrentActivityTaskPollers,
		})
	} else if options.ActivityTaskPollerBehavior != nil {
		workerParams.ActivityTaskPollerBehavior = options.ActivityTaskPollerBehavior
	} else {
		panic("must set either MaxConcurrentActivityTaskPollers or ActivityTaskPollerBehavior")
	}

	// Nexus Task Poller
	if options.MaxConcurrentNexusTaskPollers != 0 {
		workerParams.NexusTaskPollerBehavior = NewPollerBehaviorSimpleMaximum(PollerBehaviorSimpleMaximumOptions{
			MaximumNumberOfPollers: options.MaxConcurrentNexusTaskPollers,
		})
	} else if options.NexusTaskPollerBehavior != nil {
		workerParams.NexusTaskPollerBehavior = options.NexusTaskPollerBehavior
	} else {
		panic("must set either MaxConcurrentNexusTaskPollers or NexusTaskPollerBehavior")
	}

	// 最终校验 workerParams 中的必需参数是否都已正确设置
	ensureRequiredParams(&workerParams)

	// 处理测试标签（用于测试场景下注入额外参数，生产环境中通常为空）
	processTestTags(&options, &workerParams)

	// =========================================================================
	// 阶段 8：创建注册表（Registry）+ 组装拦截器链（Interceptor Chain）
	// =========================================================================
	// registry 是 Worker 级别的注册表，存储所有 Workflow/Activity/NexusService 的
	// 函数名→函数实现的映射关系。
	registry := newRegistryWithOptions(registryOptions{disableAliasing: options.DisableRegistrationAliasing})
	// 拦截器链的拼接顺序：Client 级别的拦截器在前，Worker 级别的拦截器在后。
	// 这样 Client 级别的拦截器（如 Tracing）可以包裹住 Worker 级别的拦截器。
	// 注意：用 make + append 而非直接 append，是为了避免修改 client.workerInterceptors 底层数组。
	registry.interceptors = make([]WorkerInterceptor, 0, len(client.workerInterceptors)+len(options.Interceptors))
	registry.interceptors = append(append(registry.interceptors, client.workerInterceptors...), options.Interceptors...)

	// =========================================================================
	// 阶段 9：创建子 Worker
	// =========================================================================
	// AggregatedWorker 下辖 4 种 Worker（按创建顺序）：
	//   workflowWorker  — 处理 Workflow Task（流程编排逻辑）
	//   activityWorker  — 处理 Activity Task（业务逻辑执行单元）
	//   sessionWorker   — 处理 Session（需要保证 Activity 在同一 Host 上连续执行）
	//   nexusWorker     — 延迟创建（在 Start() 中有注册 Nexus Service 时才创建）

	// --- 9a. Workflow Worker ---
	// 除非显式设置 DisableWorkflowWorker=true（纯 Activity Worker 模式），否则总是创建。
	var workflowWorker *workflowWorker
	if !options.DisableWorkflowWorker {
		testTags := getTestTags(options.BackgroundActivityContext)
		if len(testTags) > 0 {
			// 测试模式：允许注入 Pressure Points（用于模拟各种异常场景的测试钩子）
			workflowWorker = newWorkflowWorkerWithPressurePoints(client, workerParams, testTags, registry)
		} else {
			// 生产模式
			workflowWorker = newWorkflowWorker(client, workerParams, nil, registry)
		}
	}

	// --- 9b. Activity Worker ---
	// LocalActivityWorkerOnly 模式：只执行 Local Activity（不需要远程 Poll），不创建完整 Activity Worker。
	// 注意：eagerActivityExecutor 需要持有 activityWorker 引用，用于实现 Eager Activity 分发。
	var activityWorker *activityWorker
	if !options.LocalActivityWorkerOnly {
		activityWorker = newActivityWorker(client, workerParams, nil, registry, nil)
		workerParams.eagerActivityExecutor.activityWorker = activityWorker.worker
	}

	// --- 9c. Session Worker ---
	// Session Worker 保证一组 Activity 始终被同一个 Worker 实例执行，通常用于需要
	// 本地状态连续性的场景（如文件处理）。注意 Session Worker 和 LocalActivityWorkerOnly 不兼容。
	var sessionWorker *sessionWorker
	if options.EnableSessionWorker && !options.LocalActivityWorkerOnly {
		sessionWorker = newSessionWorker(client, workerParams, registry, options.MaxConcurrentSessionExecutionSize)
		// 自动注册 Session 内部使用的两个系统 Activity：
		//   sessionCreationActivity   — 在 Server 端创建 Session
		//   sessionCompletionActivity — 在 Server 端完成/释放 Session
		registry.RegisterActivityWithOptions(sessionCreationActivity, RegisterActivityOptions{
			Name: sessionCreationActivityName,
		})
		registry.RegisterActivityWithOptions(sessionCompletionActivity, RegisterActivityOptions{
			Name: sessionCompletionActivityName,
		})
	}

	// =========================================================================
	// 阶段 10：解析系统信息提供者（SysInfoProvider）
	// =========================================================================
	// SysInfoProvider 用于 Worker 心跳时报告宿主机的 CPU 和内存使用率。
	// 两个来源：
	//   a) WorkerOptions.SysInfoProvider：用户显式设置
	//   b) Tuner 的 SlotSupplier 可能实现了 HasSysInfoProvider 接口（自动提供的系统信息）
	// 冲突处理：如果两者都设置了但不是同一个实例，则 panic（ambiguous config）。
	// 如果都没设置，心跳报告中 CPU/Memory 字段为 0。
	var sysInfoProvider SysInfoProvider
	var tunerSysInfoProvider SysInfoProvider
	if sis, ok := options.Tuner.GetWorkflowTaskSlotSupplier().(HasSysInfoProvider); ok {
		tunerSysInfoProvider = sis.SysInfoProvider()
	}
	switch {
	case options.SysInfoProvider != nil && tunerSysInfoProvider != nil && options.SysInfoProvider != tunerSysInfoProvider:
		panic("WorkerOptions.SysInfoProvider conflicts with the SysInfoProvider exposed by the Tuner; " +
			"set only one, or set both to the same instance")
	case options.SysInfoProvider != nil:
		sysInfoProvider = options.SysInfoProvider
	default:
		sysInfoProvider = tunerSysInfoProvider
	}

	// =========================================================================
	// 阶段 11：构造心跳回调（heartbeatCallback）
	// =========================================================================
	// 心跳回调是一个无参函数，每次被调用时生成一个 WorkerHeartbeat protobuf 消息。
	// 它在心跳 goroutine 中和 Shutdown 路径上被并发调用，所以内部用 mu 保护可变状态。
	// 仅在 client.workerHeartbeatInterval != 0 时才创建（即用户显式启用了心跳）。
	var heartbeatCallback func() *workerpb.WorkerHeartbeat
	if client.workerHeartbeatInterval != 0 {
		// --- 11a. 初始化心跳中的静态/半静态字段 ---
		startTime := timestamppb.New(time.Now()) // Worker 启动时间（在整个心跳生命周期中不变）
		hostname, _ := os.Hostname()
		pid := strconv.Itoa(os.Getpid())
		previousHeartbeatTime := time.Now() // 上一次心跳时间（每次心跳后更新）
		pluginInfos := collectPluginInfos(client.clientPluginNames, plugins)
		driverInfos := collectStorageDriverInfos(client.storageDriverTypes)

		// --- 11b. 累计指标的"上一次快照"变量 ---
		// 这些变量用于计算两次心跳之间的增量（例如本次心跳间隔内处理了多少 task）。
		// 它们通过 populateOpts 指针传入 heartbeatMetrics.PopulateHeartbeat，
		// PopulateHeartbeat 会读取当前累计值、计算增量，然后更新这些变量为当前值。
		var prevWorkflowProcessed, prevWorkflowFailed int64
		var prevActivityProcessed, prevActivityFailed int64
		var prevLocalActivityProcessed, prevLocalActivityFailed int64
		var prevNexusProcessed, prevNexusFailed int64

		populateOpts := &populateHeartbeatOptions{
			workflowPollerBehavior:     workerParams.WorkflowTaskPollerBehavior,
			activityPollerBehavior:     workerParams.ActivityTaskPollerBehavior,
			nexusPollerBehavior:        workerParams.NexusTaskPollerBehavior,
			prevWorkflowProcessed:      &prevWorkflowProcessed,
			prevWorkflowFailed:         &prevWorkflowFailed,
			prevActivityProcessed:      &prevActivityProcessed,
			prevActivityFailed:         &prevActivityFailed,
			prevLocalActivityProcessed: &prevLocalActivityProcessed,
			prevLocalActivityFailed:    &prevLocalActivityFailed,
			prevNexusProcessed:         &prevNexusProcessed,
			prevNexusFailed:            &prevNexusFailed,
			pollTimeTracker:            workerParams.pollTimeTracker,
		}

		// --- 11c. 部署版本信息 ---
		// 如果启用了 Worker Versioning，将 DeploymentName + BuildID 附加到心跳中，
		// Server 端用于基于版本的流量路由。
		var deploymentVersion *deploymentpb.WorkerDeploymentVersion
		if options.DeploymentOptions.UseVersioning {
			deploymentVersion = &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: options.DeploymentOptions.Version.DeploymentName,
				BuildId:        options.DeploymentOptions.Version.BuildID,
			}
		}

		// --- 11d. heartbeatCallback 闭包 ---
		// 此闭包每次被调用时：
		//   1. 采集当前 CPU/内存使用率
		//   2. 读取各子 Worker 的 SlotSupplier 类型
		//   3. 计算距上次心跳的时间间隔
		//   4. 根据 shuttingDown 状态设置 Worker 状态（RUNNING / SHUTTING_DOWN）
		//   5. 填充累计指标增量（通过 heartbeatMetrics.PopulateHeartbeat）
		var mu sync.Mutex // 保护闭包内的可变状态（populateOpts 中的 slotSupplierKind + previousHeartbeatTime）
		heartbeatCallback = func() *workerpb.WorkerHeartbeat {
			cpuUsage := getCpuUsage(sysInfoProvider, workerParams.Logger)
			memUsage := getMemUsage(sysInfoProvider, workerParams.Logger)

			mu.Lock()
			defer mu.Unlock()
			// 动态读取各子 Worker 的 SlotSupplier 类型（可能在运行时变化）
			if aw.workflowWorker != nil {
				populateOpts.workflowSlotSupplierKind = aw.workflowWorker.worker.slotSupplier.GetSlotSupplierKind()
				populateOpts.localActivitySlotSupplierKind = aw.workflowWorker.localActivityWorker.slotSupplier.GetSlotSupplierKind()
			}
			if aw.activityWorker != nil {
				populateOpts.activitySlotSupplierKind = aw.activityWorker.worker.slotSupplier.GetSlotSupplierKind()
			}
			if aw.nexusWorker != nil {
				populateOpts.nexusSlotSupplierKind = aw.nexusWorker.worker.slotSupplier.GetSlotSupplierKind()
			}
			heartbeatTime := time.Now()
			elapsedSinceLastHeartbeat := heartbeatTime.Sub(previousHeartbeatTime)
			previousHeartbeatTime = heartbeatTime

			status := enumspb.WORKER_STATUS_RUNNING
			if aw.shuttingDown.Load() {
				status = enumspb.WORKER_STATUS_SHUTTING_DOWN
			}

			hb := &workerpb.WorkerHeartbeat{
				WorkerInstanceKey: aw.workerInstanceKey,
				WorkerIdentity:    aw.executionParams.Identity,
				HostInfo: &workerpb.WorkerHostInfo{
					HostName:            hostname,
					WorkerGroupingKey:   aw.client.workerGroupingKey,
					ProcessId:           pid,
					CurrentHostCpuUsage: cpuUsage,
					CurrentHostMemUsage: memUsage,
				},
				TaskQueue:                 aw.executionParams.TaskQueue,
				DeploymentVersion:         deploymentVersion,
				SdkName:                   SDKName,
				SdkVersion:                SDKVersion,
				Status:                    status,
				StartTime:                 startTime,
				HeartbeatTime:             timestamppb.New(heartbeatTime),
				ElapsedSinceLastHeartbeat: durationpb.New(elapsedSinceLastHeartbeat),
				Plugins:                   pluginInfos,
				Drivers:                   driverInfos,
			}
			// PopulateHeartbeat 会从各 Worker 的指标计数器中读取当前累计值，
			// 与 prev* 变量对比得到增量，写入心跳消息，然后更新 prev* 变量。
			aw.heartbeatMetrics.PopulateHeartbeat(hb, populateOpts)

			return hb
		}
	}

	// =========================================================================
	// 阶段 12：组装 AggregatedWorker 结构体
	// =========================================================================
	// 将前面创建的所有组件和子 Worker 聚合在一起，形成完整的 AggregatedWorker。
	aw = &AggregatedWorker{
		client:         client,
		workflowWorker: workflowWorker,
		activityWorker: activityWorker,
		sessionWorker:  sessionWorker,
		// nexusWorker 不在这里创建：它的创建时机在 start() 中（详见阶段 13 注释）。
		logger:                       workerParams.Logger,
		registry:                     registry,
		stopC:                        make(chan struct{}), // 创建一个未关闭的 channel，供 Stop() 通知用
		capabilities:                 &capabilities,       // 指针，start() 中填充
		executionParams:              workerParams,
		workerInstanceKey:            workerInstanceKey,
		plugins:                      plugins,
		pluginRegistryOptions:        &pluginRegistryOptions,
		heartbeatMetrics:             heartbeatMetrics,
		heartbeatCallback:            heartbeatCallback,
		workerPollCompleteOnShutdown: workerPollCompleteOnShutdown,
	}

	// =========================================================================
	// 阶段 13：包装 memoizedStart（插件洋葱模型 + sync.OnceValue）
	// =========================================================================
	//
	// memoizedStart 的设计分两层：
	//
	// 【外层 — sync.OnceValue】
	//   sync.OnceValue 确保 start() 函数只会被执行一次。后续调用直接返回缓存的 error。
	//   这里用 OnceValue 而非 Once(func()) error，是因为需要缓存返回值。
	//
	// 【内层 — Plugin 洋葱模型】
	//   插件列表按注册顺序排列（先 Client 后 Worker）。在 StartWorker 钩子中，
	//   期望的执行顺序是：后注册的插件先执行外层逻辑。
	//   所以这里从后往前遍历，用一个闭包链实现"洋葱皮"式的层层包裹：
	//
	//       插件N.StartWorker → 插件N-1.StartWorker → ... → 插件1.StartWorker → aw.start()
	//
	//   每个插件的 StartWorker 可以：
	//     a) 在调用 next() 之前做一些事情（如初始化资源）
	//     b) 调用 next() 触发下一层（或真正的 start）
	//     c) 在调用 next() 之后做一些事情（如注册清理钩子）
	//
	//   最后一个被包裹的是 aw.start()（真正的核心启动逻辑），它负责：
	//     - 确保与 Server 的连接已初始化
	//     - 拉取 Server Capabilities 和 Namespace 数据
	//     - 设置 Payload 大小限制
	//     - 按顺序启动 workflowWorker → activityWorker → sessionWorker → nexusWorker
	//     - 注册心跳 Worker
	//
	// 【为什么 nexusWorker 不在这里创建，而在 start() 中？】
	//   因为 start() 中会调用 registry.getRegisteredNexusServices() 检查是否有
	//   已注册的 Nexus Service。Nexus Worker 的创建由"是否注册了 Service"来决定。
	//   如果在 NewAggregatedWorker 时还没有注册任何 Service（常见场景），
	//   就使用 nil 占位。在 Start() -> start() 时，如果发现有注册的 Nexus Service，
	//   才真正创建并启动 Nexus Worker。
	aw.memoizedStart = sync.OnceValue(func() error {
		// start 初始化为真正的核心启动函数
		start := func(context.Context, WorkerPluginStartWorkerOptions) error { return aw.start() }
		// 从后往前遍历插件，将每个插件的 StartWorker 作为"洋葱皮"逐层包裹
		for i := len(plugins) - 1; i >= 0; i-- {
			plugin := plugins[i]
			next := start // 保存当前层（内层）
			start = func(ctx context.Context, options WorkerPluginStartWorkerOptions) error {
				// 调用插件的 StartWorker，传入 next 让插件决定何时调用内层
				return plugin.StartWorker(ctx, options, next)
			}
		}
		// 执行最外层的 start（即被所有插件包裹后的核心启动逻辑）
		return start(context.Background(), WorkerPluginStartWorkerOptions{
			WorkerInstanceKey: workerInstanceKey,
			WorkerRegistry:    aw,
		})
	})
	return aw
}

func processTestTags(wOptions *WorkerOptions, ep *workerExecutionParameters) {
	testTags := getTestTags(wOptions.BackgroundActivityContext)
	if testTags != nil {
		if paramsOverride, ok := testTags[workerOptionsConfig]; ok {
			for key, val := range paramsOverride {
				switch key {
				case workerOptionsConfigConcurrentPollRoutineSize:
					if size, err := strconv.Atoi(val); err == nil {
						ep.ActivityTaskPollerBehavior = NewPollerBehaviorSimpleMaximum(
							PollerBehaviorSimpleMaximumOptions{
								MaximumNumberOfPollers: size,
							},
						)
						ep.WorkflowTaskPollerBehavior = NewPollerBehaviorSimpleMaximum(
							PollerBehaviorSimpleMaximumOptions{
								MaximumNumberOfPollers: size,
							},
						)
					}
				}
			}
		}
	}
}

func isWorkflowContext(inType reflect.Type) bool {
	// NOTE: We don't expect any one to derive from workflow context.
	return inType == reflect.TypeOf((*Context)(nil)).Elem()
}

func isValidResultType(inType reflect.Type) bool {
	// https://golang.org/pkg/reflect/#Kind
	switch inType.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return false
	}

	return true
}

func isError(inType reflect.Type) bool {
	errorElem := reflect.TypeOf((*error)(nil)).Elem()
	return inType != nil && inType.Implements(errorElem)
}

func getFunctionName(i interface{}) (name string, isMethod bool) {
	if fullName, ok := i.(string); ok {
		return fullName, false
	}
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	// Full function name that has a struct pointer receiver has the following format
	// <prefix>.(*<type>).<function>
	isMethod = strings.ContainsAny(fullName, "*")
	elements := strings.Split(fullName, ".")
	shortName := elements[len(elements)-1]
	// This allows to call activities by method pointer
	// Compiler adds -fm suffix to a function name which has a receiver
	// Note that this works even if struct pointer used to get the function is nil
	// It is possible because nil receivers are allowed.
	// For example:
	// var a *Activities
	// ExecuteActivity(ctx, a.Foo)
	// will call this function which is going to return "Foo"
	return strings.TrimSuffix(shortName, "-fm"), isMethod
}

func getActivityFunctionName(r *registry, i interface{}) string {
	result, _ := getFunctionName(i)
	if alias, ok := r.getActivityAlias(result); ok {
		result = alias
	}
	return result
}

func getWorkflowFunctionName(r *registry, workflowFunc interface{}) (string, error) {
	fnName := ""
	fType := reflect.TypeOf(workflowFunc)
	switch getKind(fType) {
	case reflect.String:
		fnName = reflect.ValueOf(workflowFunc).String()
	case reflect.Func:
		fnName, _ = getFunctionName(workflowFunc)
		if alias, ok := r.getWorkflowAlias(fnName); ok {
			fnName = alias
		}
	default:
		return "", fmt.Errorf("invalid type 'workflowFunc' parameter provided, it can be either worker function or function name: %v", workflowFunc)
	}

	return fnName, nil
}

func getReadOnlyChannel(c chan struct{}) <-chan struct{} {
	return c
}

func setWorkerOptionsDefaults(options *WorkerOptions) {
	if options.Tuner != nil {
		if options.MaxConcurrentWorkflowTaskExecutionSize != 0 ||
			options.MaxConcurrentActivityExecutionSize != 0 ||
			options.MaxConcurrentLocalActivityExecutionSize != 0 ||
			options.MaxConcurrentNexusTaskExecutionSize != 0 {
			panic("cannot set MaxConcurrentWorkflowTaskExecutionSize, MaxConcurrentActivityExecutionSize, MaxConcurrentLocalActivityExecutionSize, or MaxConcurrentNexusTaskExecutionSize with Tuner")
		}
	}
	maxConcurrentWFT := options.MaxConcurrentWorkflowTaskExecutionSize
	maxConcurrentAct := options.MaxConcurrentActivityExecutionSize
	maxConcurrentLA := options.MaxConcurrentLocalActivityExecutionSize
	maxConcurrentNexus := options.MaxConcurrentNexusTaskExecutionSize
	if options.MaxConcurrentActivityExecutionSize <= 0 {
		maxConcurrentAct = defaultMaxConcurrentActivityExecutionSize
	}
	if options.WorkerActivitiesPerSecond == 0 {
		options.WorkerActivitiesPerSecond = defaultWorkerActivitiesPerSecond
	}
	if options.MaxConcurrentActivityTaskPollers != 0 && options.ActivityTaskPollerBehavior != nil {
		panic("cannot set both MaxConcurrentActivityTaskPollers and ActivityTaskPollerBehavior")
	} else if options.ActivityTaskPollerBehavior == nil && options.MaxConcurrentActivityTaskPollers <= 0 {
		options.MaxConcurrentActivityTaskPollers = defaultConcurrentPollRoutineSize
	}
	if options.MaxConcurrentWorkflowTaskExecutionSize <= 0 {
		maxConcurrentWFT = defaultMaxConcurrentTaskExecutionSize
	}
	if options.MaxConcurrentWorkflowTaskPollers != 0 && options.WorkflowTaskPollerBehavior != nil {
		panic("cannot set both MaxConcurrentWorkflowTaskPollers and WorkflowTaskPollerBehavior")
	} else if options.WorkflowTaskPollerBehavior == nil && options.MaxConcurrentWorkflowTaskPollers <= 0 {
		options.MaxConcurrentWorkflowTaskPollers = defaultConcurrentPollRoutineSize
	}
	if options.MaxConcurrentLocalActivityExecutionSize <= 0 {
		maxConcurrentLA = defaultMaxConcurrentLocalActivityExecutionSize
	}
	if options.WorkerLocalActivitiesPerSecond == 0 {
		options.WorkerLocalActivitiesPerSecond = defaultWorkerLocalActivitiesPerSecond
	}
	if options.TaskQueueActivitiesPerSecond == 0 {
		options.TaskQueueActivitiesPerSecond = defaultTaskQueueActivitiesPerSecond
	} else {
		// Disable eager activities when the task queue rate limit is set because
		// the server does not rate limit eager activities.
		options.DisableEagerActivities = true
	}
	if options.MaxConcurrentNexusTaskPollers != 0 && options.NexusTaskPollerBehavior != nil {
		panic("cannot set both MaxConcurrentNexusTaskExecutionSize and NexusTaskPollerBehavior")
	} else if options.NexusTaskPollerBehavior == nil && options.MaxConcurrentNexusTaskPollers <= 0 {
		options.MaxConcurrentNexusTaskPollers = defaultConcurrentPollRoutineSize
	}
	if options.MaxConcurrentNexusTaskExecutionSize <= 0 {
		maxConcurrentNexus = defaultMaxConcurrentTaskExecutionSize
	}
	if options.StickyScheduleToStartTimeout.Seconds() == 0 {
		options.StickyScheduleToStartTimeout = stickyWorkflowTaskScheduleToStartTimeoutSeconds * time.Second
	}
	if options.MaxConcurrentSessionExecutionSize == 0 {
		options.MaxConcurrentSessionExecutionSize = defaultMaxConcurrentSessionExecutionSize
	}
	if options.DeadlockDetectionTimeout == 0 {
		if debugMode {
			options.DeadlockDetectionTimeout = unlimitedDeadlockDetectionTimeout
		} else {
			options.DeadlockDetectionTimeout = defaultDeadlockDetectionTimeout
		}
	}
	if options.DefaultHeartbeatThrottleInterval == 0 {
		options.DefaultHeartbeatThrottleInterval = defaultDefaultHeartbeatThrottleInterval
	}
	if options.MaxHeartbeatThrottleInterval == 0 {
		options.MaxHeartbeatThrottleInterval = defaultMaxHeartbeatThrottleInterval
	}
	if options.MaxConcurrentWorkflowTaskExternalStorageVisits == 0 {
		options.MaxConcurrentWorkflowTaskExternalStorageVisits = defaultMaxConcurrentWorkflowTaskExternalStorageVisits
	}
	if options.Tuner == nil {
		// Err cannot happen since these slot numbers are guaranteed valid
		options.Tuner, _ = NewFixedSizeTuner(FixedSizeTunerOptions{
			NumWorkflowSlots:      maxConcurrentWFT,
			NumActivitySlots:      maxConcurrentAct,
			NumLocalActivitySlots: maxConcurrentLA,
			NumNexusSlots:         maxConcurrentNexus})

	}
}

// setClientDefaults should be needed only in unit tests.
func setClientDefaults(client *WorkflowClient) {
	if client.dataConverter == nil {
		client.dataConverter = converter.GetDefaultDataConverter()
	}
	if client.namespace == "" {
		client.namespace = DefaultNamespace
	}
	if client.metricsHandler == nil {
		client.metricsHandler = metrics.NopHandler
	}
}

// getTestTags returns the test tags in the context.
func getTestTags(ctx context.Context) map[string]map[string]string {
	if ctx != nil {
		env := ctx.Value(testTagsContextKey)
		if env != nil {
			return env.(map[string]map[string]string)
		}
	}
	return nil
}

// Same as executeFunction but injects the workflow context as the first
// parameter if the function takes it (regardless of existing parameters).
func executeFunctionWithWorkflowContext(ctx Context, fn interface{}, args []interface{}) (interface{}, error) {
	if fnType := reflect.TypeOf(fn); fnType.NumIn() > 0 && isWorkflowContext(fnType.In(0)) {
		args = append([]interface{}{ctx}, args...)
	}
	return executeFunction(fn, args)
}

// Same as executeFunction but injects the context as the first parameter if the
// function takes it (regardless of existing parameters).
func executeFunctionWithContext(ctx context.Context, fn interface{}, args []interface{}) (interface{}, error) {
	if fnType := reflect.TypeOf(fn); fnType.NumIn() > 0 && isActivityContext(fnType.In(0)) {
		args = append([]interface{}{ctx}, args...)
	}
	return executeFunction(fn, args)
}

// Executes function and ensures that there is always 1 or 2 results and second
// result is error.
func executeFunction(fn interface{}, args []interface{}) (interface{}, error) {
	fnValue := reflect.ValueOf(fn)
	reflectArgs := make([]reflect.Value, len(args))
	for i, arg := range args {
		// If the argument is nil, use zero value
		if arg == nil {
			reflectArgs[i] = reflect.New(fnValue.Type().In(i)).Elem()
		} else {
			reflectArgs[i] = reflect.ValueOf(arg)
		}
	}
	retValues := fnValue.Call(reflectArgs)

	// Expect either error or (result, error)
	if len(retValues) == 0 || len(retValues) > 2 {
		fnName, _ := getFunctionName(fn)
		return nil, fmt.Errorf(
			"the function: %v signature returns %d results, it is expecting to return either error or (result, error)",
			fnName, len(retValues))
	}
	// Convert error
	var err error
	if errResult := retValues[len(retValues)-1].Interface(); errResult != nil {
		var ok bool
		if err, ok = errResult.(error); !ok {
			return nil, fmt.Errorf(
				"failed to serialize error result as it is not of error interface: %v",
				errResult)
		}
	}
	// If there are two results, convert the first only if it's not a nil pointer
	var res interface{}
	if len(retValues) > 1 && (retValues[0].Kind() != reflect.Ptr || !retValues[0].IsNil()) {
		res = retValues[0].Interface()
	}
	return res, err
}

func workerDeploymentVersionFromProto(wd *deploymentpb.WorkerDeploymentVersion) WorkerDeploymentVersion {
	return WorkerDeploymentVersion{
		DeploymentName: wd.DeploymentName,
		BuildID:        wd.BuildId,
	}
}

func (wd *WorkerDeploymentVersion) toProto() *deploymentpb.WorkerDeploymentVersion {
	return &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: wd.DeploymentName,
		BuildId:        wd.BuildID,
	}
}

func (wd *WorkerDeploymentVersion) toCanonicalString() string {
	return fmt.Sprintf("%s.%s", wd.DeploymentName, wd.BuildID)
}

func workerDeploymentVersionFromString(version string) *WorkerDeploymentVersion {
	if splitVersion := strings.SplitN(version, ".", 2); len(splitVersion) == 2 {
		return &WorkerDeploymentVersion{
			DeploymentName: splitVersion[0],
			BuildID:        splitVersion[1],
		}
	}
	return nil
}

func workerDeploymentVersionFromProtoOrString(wd *deploymentpb.WorkerDeploymentVersion, fallback string) *WorkerDeploymentVersion {
	if wd == nil {
		return workerDeploymentVersionFromString(fallback)
	}
	return &WorkerDeploymentVersion{
		DeploymentName: wd.DeploymentName,
		BuildID:        wd.BuildId,
	}
}

func getCpuUsage(supplier SysInfoProvider, logger log.Logger) float32 {
	if supplier == nil {
		return 0
	}
	cpu, err := supplier.CpuUsage(&SysInfoContext{Logger: logger})
	if err != nil {
		logger.Warn("Failed to get CPU usage for heartbeat", "error", err)
		return 0
	}
	return float32(cpu)
}

func getMemUsage(supplier SysInfoProvider, logger log.Logger) float32 {
	if supplier == nil {
		return 0
	}
	mem, err := supplier.MemoryUsage(&SysInfoContext{Logger: logger})
	if err != nil {
		logger.Warn("Failed to get memory usage for heartbeat", "error", err)
		return 0
	}
	return float32(mem)
}

// collectPluginInfos collects plugin names from client and worker plugins,
// deduplicates them, and returns a slice of PluginInfo for heartbeat reporting.
func collectPluginInfos(clientPluginNames []string, workerPlugins []WorkerPlugin) []*workerpb.PluginInfo {
	set := make(map[string]struct{}, len(clientPluginNames)+len(workerPlugins))
	result := make([]*workerpb.PluginInfo, 0, len(clientPluginNames)+len(workerPlugins))
	for _, name := range clientPluginNames {
		if _, found := set[name]; !found {
			set[name] = struct{}{}
			result = append(result, &workerpb.PluginInfo{Name: name})
		}
	}
	for _, plugin := range workerPlugins {
		if _, found := set[plugin.Name()]; !found {
			set[plugin.Name()] = struct{}{}
			result = append(result, &workerpb.PluginInfo{Name: plugin.Name()})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

func collectStorageDriverInfos(driverTypes []string) []*workerpb.StorageDriverInfo {
	if len(driverTypes) == 0 {
		return nil
	}
	result := make([]*workerpb.StorageDriverInfo, len(driverTypes))
	for i, t := range driverTypes {
		result[i] = &workerpb.StorageDriverInfo{Type: t}
	}
	return result
}
