package internal

// All code in this file is private to the package.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/sdk/internal/common/retry"
	"go.temporal.io/sdk/internal/extstore"
	"go.temporal.io/sdk/internal/protocol"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/internal/common/util"
	"go.temporal.io/sdk/log"
)

const (
	defaultStickyCacheSize = 10000

	noRetryBackoff = time.Duration(-1)

	defaultDefaultHeartbeatThrottleInterval               = 30 * time.Second
	defaultMaxHeartbeatThrottleInterval                   = 60 * time.Second
	defaultMaxConcurrentWorkflowTaskExternalStorageVisits = 3
)

var (
	// ErrActivityPaused is returned from an activity heartbeat or the cause of an activity's context to indicate that the activity is paused.
	//
	// WARNING: Activity pause is currently experimental
	ErrActivityPaused = errors.New("activity paused")

	// ErrActivityReset is returned from an activity heartbeat or the cause of an activity's context to indicate that the activity has been reset.
	//
	// WARNING: Activity reset is currently experimental
	ErrActivityReset = errors.New("activity reset")
)

type (
	// workflowExecutionEventHandler process a single event.
	workflowExecutionEventHandler interface {
		// Process a single event and return the assosciated commands.
		// Return List of commands made, any error.
		ProcessEvent(event *historypb.HistoryEvent, isReplay bool, isLast bool) error
		// ProcessInteraction processes interaction inputs
		ProcessMessage(msg *protocolpb.Message, isReplay bool, isLast bool) error
		// ProcessQuery process a query request.
		ProcessQuery(queryType string, queryArgs *commonpb.Payloads, header *commonpb.Header) (*commonpb.Payloads, error)
		StackTrace() string
		// Close for cleaning up resources on this event handler
		Close()
	}

	// workflowTask wraps a workflow task.
	workflowTask struct {
		task            *workflowservice.PollWorkflowTaskQueueResponse
		historyIterator HistoryIterator
		doneCh          chan struct{}
		laResultCh      chan *localActivityResult

		// This channel must be initialized with a one-size buffer and is used to indicate when
		// it is time for a local activity to be retried
		laRetryCh chan *localActivityTask
	}

	// eagerWorkflowTask represents a workflow task sent from an eager workflow executor
	eagerWorkflowTask struct {
		task *workflowservice.PollWorkflowTaskQueueResponse
	}

	// activityTask wraps a activity task.
	activityTask struct {
		task   *workflowservice.PollActivityTaskQueueResponse
		permit *SlotPermit
	}

	// workflowExecutionContextImpl is the cached workflow state for sticky execution
	workflowExecutionContextImpl struct {
		mutex        sync.Mutex
		workflowInfo *WorkflowInfo
		wth          *workflowTaskHandlerImpl

		eventHandler *workflowExecutionEventHandler

		isWorkflowCompleted bool
		result              *commonpb.Payloads
		err                 error
		// previousStartedEventID is the event ID of the workflow task started event of the previous workflow task.
		previousStartedEventID int64
		// lastHandledEventID is the event ID of the last event that the workflow state machine processed.
		lastHandledEventID int64

		newCommands         []*commandpb.Command
		newMessages         []*protocolpb.Message
		currentWorkflowTask *workflowservice.PollWorkflowTaskQueueResponse
		laTunnel            *localActivityTunnel
		cached              bool
	}

	// workflowTaskHandlerImpl is the implementation of WorkflowTaskHandler
	workflowTaskHandlerImpl struct {
		namespace                 string
		metricsHandler            metrics.Handler
		ppMgr                     pressurePointMgr
		logger                    log.Logger
		identity                  string
		workerBuildID             string
		useBuildIDForVersioning   bool
		workerDeploymentVersion   WorkerDeploymentVersion
		defaultVersioningBehavior VersioningBehavior
		enableLoggingInReplay     bool
		registry                  *registry
		laTunnel                  *localActivityTunnel
		workflowPanicPolicy       WorkflowPanicPolicy
		dataConverter             converter.DataConverter
		failureConverter          converter.FailureConverter
		contextPropagators        []ContextPropagator
		cache                     *WorkerCache
		deadlockDetectionTimeout  time.Duration
		capabilities              *workflowservice.GetSystemInfoResponse_Capabilities
	}

	activityProvider func(name string) activity

	// activityTaskHandlerImpl is the implementation of ActivityTaskHandler
	activityTaskHandlerImpl struct {
		taskQueueName                    string
		identity                         string
		client                           *WorkflowClient
		metricsHandler                   metrics.Handler
		logger                           log.Logger
		backgroundContext                context.Context
		registry                         *registry
		activityProvider                 activityProvider
		dataConverter                    converter.DataConverter
		failureConverter                 converter.FailureConverter
		workerStopCh                     <-chan struct{}
		contextPropagators               []ContextPropagator
		namespace                        string
		defaultHeartbeatThrottleInterval time.Duration
		maxHeartbeatThrottleInterval     time.Duration
		versionStamp                     *commonpb.WorkerVersionStamp
		deployment                       *deploymentpb.Deployment
		workerDeploymentOptions          *deploymentpb.WorkerDeploymentOptions
		inboundPayloadVisitor            PayloadVisitor
		outboundPayloadVisitor           PayloadVisitor
		payloadVisitorConcurrency        int
	}

	// history wrapper method to help information about events.
	history struct {
		workflowTask       *workflowTask
		eventsHandler      *workflowExecutionEventHandlerImpl
		loadedEvents       []*historypb.HistoryEvent
		currentIndex       int
		nextEventID        int64 // next expected eventID for sanity
		lastEventID        int64 // last expected eventID, zero indicates read until end of stream
		lastHandledEventID int64 // last event ID that was processed
		next               []*historypb.HistoryEvent
		nextMessages       []*protocolpb.Message
		nextFlags          []sdkFlag
		binaryChecksum     string
		sdkVersion         string
		sdkName            string
	}

	workflowTaskHeartbeatError struct {
		Message string
	}

	historyMismatchError struct {
		message string
	}

	unknownSdkFlagError struct {
		message string
	}

	preparedTask struct {
		events         []*historypb.HistoryEvent
		markers        []*historypb.HistoryEvent
		flags          []sdkFlag
		acceptedMsgs   []*protocolpb.Message
		admittedMsgs   []*protocolpb.Message
		binaryChecksum string
		sdkVersion     string
		sdkName        string
		// Is null if there was no task completed event to read the build ID from (but may be
		// empty string if there was, and it was empty)
		buildID *string
	}

	finishedTask struct {
		isFailed       bool
		binaryChecksum string
		flags          []sdkFlag
		sdkVersion     string
		sdkName        string
	}

	workflowTaskCompletion struct {
		rawRequest             proto.Message
		applyCompletionMetrics func()
	}
)

func newHistory(lastHandledEventID int64, task *workflowTask, eventsHandler *workflowExecutionEventHandlerImpl) *history {
	result := &history{
		workflowTask:       task,
		eventsHandler:      eventsHandler,
		loadedEvents:       task.task.History.Events,
		currentIndex:       0,
		lastEventID:        task.task.GetStartedEventId(),
		lastHandledEventID: lastHandledEventID,
	}
	if len(result.loadedEvents) > 0 {
		result.nextEventID = result.loadedEvents[0].GetEventId()
	}
	return result
}

func (e workflowTaskHeartbeatError) Error() string {
	return e.Message
}

func historyMismatchErrorf(f string, v ...interface{}) historyMismatchError {
	return historyMismatchError{message: fmt.Sprintf(f, v...)}
}

func (h historyMismatchError) Error() string {
	return h.message
}

func (s unknownSdkFlagError) Error() string {
	return s.message
}

// Get workflow start event.
func (eh *history) GetWorkflowStartedEvent() (*historypb.HistoryEvent, error) {
	events := eh.workflowTask.task.History.Events
	if len(events) == 0 || events[0].GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		return nil, errors.New("unable to find WorkflowExecutionStartedEventAttributes in the history")
	}
	return events[0], nil
}

func (eh *history) IsReplayEvent(event *historypb.HistoryEvent) bool {
	return event.GetEventId() <= eh.workflowTask.task.GetPreviousStartedEventId() || isCommandEvent(event.GetEventType())
}

// isNextWorkflowTaskFailed checks if the workflow task failed or completed. If it did complete returns some information
// on the completed workflow task.
func (eh *history) isNextWorkflowTaskFailed() (task finishedTask, err error) {
	nextIndex := eh.currentIndex + 1
	// Server can return an empty page so if we need the next event we must keep checking until we either get it
	// or know we have no more pages to check
	for nextIndex >= len(eh.loadedEvents) && eh.hasMoreEvents() { // current page ends and there is more pages
		if err := eh.loadMoreEvents(); err != nil {
			return finishedTask{}, err
		}
	}

	// If not replaying we should not expect to find any more events
	if nextIndex < len(eh.loadedEvents) {
		nextEvent := eh.loadedEvents[nextIndex]
		nextEventType := nextEvent.GetEventType()
		isFailed := nextEventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT || nextEventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
		var binaryChecksum string
		var flags []sdkFlag
		if nextEventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			completedAttrs := nextEvent.GetWorkflowTaskCompletedEventAttributes()
			//lint:ignore SA1019 ignore deprecated versioning APIs
			binaryChecksum = completedAttrs.BinaryChecksum
			for _, flag := range completedAttrs.GetSdkMetadata().GetLangUsedFlags() {
				f := sdkFlagFromUint(flag)
				if !f.isValid() {
					// If a flag is not recognized (value is too high or not defined), it must fail the workflow task
					return finishedTask{}, unknownSdkFlagError{
						message: fmt.Sprintf("unknown SDK flag: %d", flag),
					}
				}
				flags = append(flags, f)
			}
		}
		return finishedTask{
			isFailed:       isFailed,
			binaryChecksum: binaryChecksum,
			flags:          flags,
			sdkName:        nextEvent.GetWorkflowTaskCompletedEventAttributes().GetSdkMetadata().GetSdkName(),
			sdkVersion:     nextEvent.GetWorkflowTaskCompletedEventAttributes().GetSdkMetadata().GetSdkVersion(),
		}, nil
	}
	return finishedTask{}, nil
}

func (eh *history) loadMoreEvents() error {
	historyPage, err := eh.getMoreEvents()
	if err != nil {
		return err
	}
	eh.loadedEvents = append(eh.loadedEvents, historyPage.Events...)
	if eh.nextEventID == 0 && len(eh.loadedEvents) > 0 {
		eh.nextEventID = eh.loadedEvents[0].GetEventId()
	}
	return nil
}

func isCommandEvent(eventType enumspb.EventType) bool {
	switch eventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		enumspb.EVENT_TYPE_TIMER_CANCELED,
		enumspb.EVENT_TYPE_MARKER_RECORDED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED:
		return true
	default:
		return false
	}
}

// nextTask returns the next task to be processed.
func (eh *history) nextTask() (*preparedTask, error) {
	if eh.next == nil {
		firstTask, err := eh.prepareTask()
		if err != nil {
			return nil, err
		}
		eh.next = firstTask.events
		eh.nextMessages = firstTask.admittedMsgs
		eh.nextFlags = firstTask.flags
		eh.sdkName = firstTask.sdkName
		eh.sdkVersion = firstTask.sdkVersion
	}

	result := eh.next
	requestMessages := eh.nextMessages
	checksum := eh.binaryChecksum
	sdkFlags := eh.nextFlags
	sdkName := eh.sdkName
	sdkVersion := eh.sdkVersion

	var markers []*historypb.HistoryEvent
	var acceptedMsgs []*protocolpb.Message
	var buildID *string
	if len(result) > 0 {
		nextTaskEvents, err := eh.prepareTask()
		if err != nil {
			return nil, err
		}
		eh.next = nextTaskEvents.events
		eh.nextMessages = nextTaskEvents.admittedMsgs
		eh.nextFlags = nextTaskEvents.flags
		eh.sdkName = nextTaskEvents.sdkName
		eh.sdkVersion = nextTaskEvents.sdkVersion
		markers = nextTaskEvents.markers
		acceptedMsgs = nextTaskEvents.acceptedMsgs
		buildID = nextTaskEvents.buildID
	}
	return &preparedTask{
		events:         result,
		markers:        markers,
		flags:          sdkFlags,
		acceptedMsgs:   acceptedMsgs,
		admittedMsgs:   requestMessages,
		binaryChecksum: checksum,
		sdkName:        sdkName,
		sdkVersion:     sdkVersion,
		buildID:        buildID,
	}, nil
}

func (eh *history) hasMoreEvents() bool {
	historyIterator := eh.workflowTask.historyIterator
	return historyIterator != nil && historyIterator.HasNextPage()
}

func (eh *history) getMoreEvents() (*historypb.History, error) {
	return eh.workflowTask.historyIterator.GetNextPage()
}

func (eh *history) verifyAllEventsProcessed() error {
	if eh.lastEventID > 0 && eh.nextEventID <= eh.lastEventID {
		return fmt.Errorf(
			"history_events: premature end of stream, expectedLastEventID=%v but no more events after eventID=%v",
			eh.lastEventID,
			eh.nextEventID-1)
	}
	if eh.lastEventID > 0 && eh.nextEventID != (eh.lastEventID+1) {
		eh.eventsHandler.logger.Warn(
			"history_events: processed events past the expected lastEventID",
			"expectedLastEventID", eh.lastEventID,
			"processedLastEventID", eh.nextEventID-1)
	}
	return nil
}

func (eh *history) prepareTask() (*preparedTask, error) {
	// 如果当前页已经读到末尾，并且 historyIterator 也没有下一页，
	// 说明没有更多 history event 可以整理成 task。
	if eh.currentIndex == len(eh.loadedEvents) && !eh.hasMoreEvents() {
		// 返回空 preparedTask 前，先校验事件流是否完整。
		// startedEventId/lastEventID 告诉 SDK 这次 WFT 理论上应该读到哪里；
		// 如果提前没事件了，verifyAllEventsProcessed 会返回错误。
		if err := eh.verifyAllEventsProcessed(); err != nil {
			return nil, err
		}
		// 空 preparedTask 表示 history 已处理完，调用方 nextTask/ProcessWorkflowTask 可以结束事件循环。
		return &preparedTask{}, nil
	}

	// taskEvents 是本次整理出来的一段 history。
	// 它不是 server 的原始 Workflow Task，而是 SDK 为 replay/执行切出来的一个“处理片段”：
	// - events: 要按顺序喂给 eventHandler.ProcessEvent 的 history events
	// - markers: 预加载出来、稍后单独应用的 MarkerRecorded events
	// - accepted/admitted messages: update protocol 相关 messages
	// - flags/sdkName/sdkVersion/buildID: 从 WorkflowTaskCompleted 等事件提取的 SDK/worker 元数据
	var taskEvents preparedTask
	// OrderEvents 这个 label 用来在遇到一个有效 WorkflowTaskStarted 边界时跳出外层循环。
	// 换句话说，prepareTask 每次最多整理到“下一个需要执行/恢复 workflow goroutine 的 WFT started”为止。
OrderEvents:
	for {
		// 如果当前 loadedEvents 这一页已经读完，就尝试从 historyIterator 再拉一页。
		// PollWorkflowTaskQueueResponse 可能只带 maxPageSize 内的一页 history；后续页要 SDK 自己继续拉。
		for eh.currentIndex == len(eh.loadedEvents) {
			// 没有更多页，说明 history 流到头了。
			if !eh.hasMoreEvents() {
				// 到头时仍然要检查是否真的读到了 server 声明的 startedEventId/lastEventID。
				if err := eh.verifyAllEventsProcessed(); err != nil {
					return nil, err
				}
				// 没有更多事件可整理，退出 OrderEvents，返回当前已收集的 taskEvents。
				break OrderEvents
			}
			// 从 historyIterator 取下一页 history，并 append 到 eh.loadedEvents。
			// 如果 server/网络/分页 token 出错，这里直接把错误返回给上层，让本次 WFT 处理失败。
			if err := eh.loadMoreEvents(); err != nil {
				return nil, err
			}
		}

		// 取当前要处理的 history event。
		event := eh.loadedEvents[eh.currentIndex]
		// eventID 是 Temporal history 的全局递增编号。
		eventID := event.GetEventId()
		// nextEventID 是 SDK 期待看到的下一个 eventID。
		// 如果不相等，说明 history 中间缺 event 或分页顺序错了，不能继续 replay。
		if eventID != eh.nextEventID {
			err := fmt.Errorf(
				"missing history events, expectedNextEventID=%v but receivedNextEventID=%v",
				eh.nextEventID, eventID)
			return nil, err
		}

		// 当前 eventID 校验通过，推进下一个期待的 eventID。
		eh.nextEventID++
		// 如果这个 event 已经在 sticky cache 的上一次处理里应用过，就跳过。
		// 注意仍然要递增 nextEventID/currentIndex，因为事件流完整性已经被确认了；
		// 这里只是不重复交给 eventHandler.ProcessEvent。
		if eventID <= eh.lastHandledEventID {
			eh.currentIndex++
			continue
		}
		// 记录当前最新处理到的 eventID。
		// ProcessWorkflowTask 外层 defer 会把这个值保存回 workflowExecutionContext。
		eh.lastHandledEventID = eventID

		// 根据 event 类型决定它要不要进入当前 preparedTask，以及是否形成 WFT 边界。
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
			// 遇到 WorkflowTaskStarted 时，要看紧跟着的下一个事件是什么。
			// 如果下一个事件是 WorkflowTaskFailed/TimedOut，说明这个 WFT 没有成功完成；
			// 这种 failed/timed-out WFT 对用户 workflow 逻辑没有推进意义，SDK 会继续往后找下一个可用 WFT。
			// 如果下一个事件不是失败/超时，说明这是一个真正要 replay/执行的 WFT started 边界。
			finishedTask, err1 := eh.isNextWorkflowTaskFailed()
			if err1 != nil {
				err := err1
				return nil, err
			}
			// 只有非 failed 的 WFT started 才作为本次 preparedTask 的结束点。
			if !finishedTask.isFailed {
				// WorkflowTaskCompleted 里可能记录了 binary checksum/build 信息；
				// ProcessWorkflowTask 后面会把它写入 workflowInfo.BinaryChecksum。
				eh.binaryChecksum = finishedTask.binaryChecksum
				// 当前 WorkflowTaskStarted 本身也要交给 eventHandler.ProcessEvent。
				// 它通常会触发 workflowDefinition.OnWorkflowTaskStarted，
				// 让 workflow goroutine 在 replay 到这个 WFT 时继续运行。
				eh.currentIndex++
				taskEvents.events = append(taskEvents.events, event)
				// 把这个 WFT completed metadata 里的 SDK flags 带到 preparedTask。
				// 后面 ProcessWorkflowTask 会恢复这些 flags，保证按历史 SDK 语义 replay。
				taskEvents.flags = append(taskEvents.flags, finishedTask.flags...)
				// 如果历史里记录了 sdkName，则带出去。
				if finishedTask.sdkName != "" {
					taskEvents.sdkName = finishedTask.sdkName
				}
				// 如果历史里记录了 sdkVersion，则带出去。
				if finishedTask.sdkVersion != "" {
					taskEvents.sdkVersion = finishedTask.sdkVersion
				}
				// 找到一个完整 WFT started 边界，本次 prepareTask 到此结束。
				// 后面的 event 留给下一次 prepareTask/nextTask 处理。
				break OrderEvents
			}
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
			// 这些事件不直接驱动用户 workflow 代码。
			// WorkflowTaskScheduled 只是 server 安排了一个 WFT；
			// WorkflowTaskTimedOut 已经在 isNextWorkflowTaskFailed 判断 failed WFT 时使用。
			// 因此这里不放进 taskEvents.events。
		default:
			// 其它事件通常都要保留到 taskEvents.events 里，让 ProcessWorkflowTask 后续按顺序应用。
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				// WorkflowTaskCompleted 里可能携带 worker deployment/build id。
				// SDK 要把 buildID 提取出来，让 replay 时 workflowInfo.currentTaskBuildID 和历史一致。
				bidStr := event.GetWorkflowTaskCompletedEventAttributes().
					GetDeploymentVersion().GetBuildId()
				if bidStr == "" {
					// 兼容旧字段 WorkerDeploymentVersion。
					// 老格式通常是 deploymentName.buildID，这里取点号后面的 buildID。
					//lint:ignore SA1019 ignore deprecated versioning APIs
					version := event.GetWorkflowTaskCompletedEventAttributes().GetWorkerDeploymentVersion()
					if splitVersion := strings.SplitN(version, ".", 2); len(splitVersion) == 2 {
						bidStr = splitVersion[1]
					}
				}
				if bidStr == "" {
					// 再兼容更旧的 WorkerVersion.BuildId 字段。
					//lint:ignore SA1019 ignore deprecated versioning APIs
					bidStr = event.GetWorkflowTaskCompletedEventAttributes().
						GetWorkerVersion().GetBuildId()
				}
				// 注意这里即使 bidStr 为空，也保存 &bidStr。
				// preparedTask.buildID 的 nil/非 nil 区分“没有 WFT completed 事件可读 build id”
				// 和“有 WFT completed，但 build id 字段为空”。
				taskEvents.buildID = &bidStr
			} else if isPreloadMarkerEvent(event) {
				// MarkerRecorded 事件先不直接放进 events 主序列，而是放到 markers。
				// ProcessWorkflowTask 会先处理非 LocalActivity marker，再处理普通 events，
				// 最后处理 LocalActivity marker，保证 Local Activity 结果在 WFT started 后恢复。
				taskEvents.markers = append(taskEvents.markers, event)
			} else if attrs := event.GetWorkflowExecutionUpdateAcceptedEventAttributes(); attrs != nil {
				// UpdateAccepted history event 可以还原出一个 protocol message。
				// replay 时 eventHandler.ProcessMessage 需要看到这个 message，才能恢复 update handler 的执行。
				taskEvents.acceptedMsgs = append(taskEvents.acceptedMsgs, inferMessageFromAcceptedEvent(attrs))
			} else if attrs := event.GetWorkflowExecutionUpdateAdmittedEventAttributes(); attrs != nil {
				// UpdateAdmitted 也要转换成 protocol message。
				// 这里手动构造 message，并把 SequencingId 绑定到当前 history eventID。
				updateID := attrs.GetRequest().GetMeta().GetUpdateId()
				taskEvents.admittedMsgs = append(taskEvents.admittedMsgs, &protocolpb.Message{
					// message ID 使用 updateID/request，表示这是 update request 消息。
					Id:                 updateID + "/request",
					ProtocolInstanceId: updateID,
					SequencingId: &protocolpb.Message_EventId{
						EventId: event.GetEventId(),
					},
					// Body 是 update request 的 protobuf Any 编码。
					Body: protocol.MustMarshalAny(attrs.GetRequest()),
				})
			}
			// 除了前面显式跳过的 WFT scheduled/timed out，以及被单独预加载的 marker 外，
			// 当前 event 仍然加入 events 主序列，后面交给 eventHandler.ProcessEvent。
			taskEvents.events = append(taskEvents.events, event)
		}
		// 当前 event 处理完，移动到 loadedEvents 的下一个下标。
		eh.currentIndex++
	}

	// 丢弃已经消费过的 loadedEvents 前缀，只保留还没处理的尾部。
	// 这样大 history replay 时，已经处理的 event slice 可以被 GC 回收，避免内存一直增长。
	eh.loadedEvents = append(
		make(
			[]*historypb.HistoryEvent,
			0,
			len(eh.loadedEvents)-eh.currentIndex),
		eh.loadedEvents[eh.currentIndex:]...,
	)

	// loadedEvents 已经被裁剪，currentIndex 要重新从 0 开始指向保留下来的第一条事件。
	eh.currentIndex = 0

	// 返回本次整理出的 preparedTask。
	// nextTask 会把当前段和下一段的 markers/messages 组合起来，交给 ProcessWorkflowTask 使用。
	return &taskEvents, nil
}

func isPreloadMarkerEvent(event *historypb.HistoryEvent) bool {
	return event.GetEventType() == enumspb.EVENT_TYPE_MARKER_RECORDED
}

func inferMessageFromAcceptedEvent(attrs *historypb.WorkflowExecutionUpdateAcceptedEventAttributes) *protocolpb.Message {
	return &protocolpb.Message{
		Id:                 attrs.GetAcceptedRequestMessageId(),
		ProtocolInstanceId: attrs.GetProtocolInstanceId(),
		SequencingId: &protocolpb.Message_EventId{
			EventId: attrs.GetAcceptedRequestSequencingEventId(),
		},
		Body: protocol.MustMarshalAny(attrs.GetAcceptedRequest()),
	}
}

// newWorkflowTaskHandler returns an implementation of workflow task handler.
func newWorkflowTaskHandler(params workerExecutionParameters, ppMgr pressurePointMgr, registry *registry) WorkflowTaskHandler {
	ensureRequiredParams(&params)
	return &workflowTaskHandlerImpl{
		namespace:                 params.Namespace,
		logger:                    params.Logger,
		ppMgr:                     ppMgr,
		metricsHandler:            params.MetricsHandler,
		identity:                  params.Identity,
		workerBuildID:             params.getBuildID(),
		useBuildIDForVersioning:   params.UseBuildIDForVersioning,
		workerDeploymentVersion:   params.DeploymentOptions.Version,
		defaultVersioningBehavior: params.DeploymentOptions.DefaultVersioningBehavior,
		enableLoggingInReplay:     params.EnableLoggingInReplay,
		registry:                  registry,
		workflowPanicPolicy:       params.WorkflowPanicPolicy,
		dataConverter:             params.DataConverter,
		failureConverter:          params.FailureConverter,
		contextPropagators:        params.ContextPropagators,
		cache:                     params.cache,
		deadlockDetectionTimeout:  params.DeadlockDetectionTimeout,
		capabilities:              params.capabilities,
	}
}

func newWorkflowExecutionContext(
	workflowInfo *WorkflowInfo,
	taskHandler *workflowTaskHandlerImpl,
) *workflowExecutionContextImpl {
	workflowContext := &workflowExecutionContextImpl{
		workflowInfo: workflowInfo,
		wth:          taskHandler,
	}
	workflowContext.createEventHandler()
	return workflowContext
}

// Lock acquires the lock on this context object, use Unlock(error) to release
// the lock.
func (w *workflowExecutionContextImpl) Lock() {
	w.mutex.Lock()
}

// Unlock cleans up after the provided error and its own internal view of the
// workflow error state by clearing itself and removing itself from cache as
// needed. It is an error to call this function without having called the Lock
// function first and the behavior is undefined. Regardless of the error
// handling involved, the context will be unlocked when this call returns.
func (w *workflowExecutionContextImpl) Unlock(err error) {
	defer w.mutex.Unlock()
	if err != nil || w.err != nil || w.isWorkflowCompleted ||
		(w.wth.cache.MaxWorkflowCacheSize() <= 0 && !w.hasPendingLocalActivityWork()) {
		// TODO: in case of closed, it assumes the close command always succeed. need server side change to return
		// error to indicate the close failure case. This should be a rare case. For now, always remove the cache, and
		// if the close command failed, the next command will have to rebuild the state.
		if w.wth.cache.getWorkflowCache().Exist(w.workflowInfo.WorkflowExecution.RunID) {
			w.wth.cache.removeWorkflowContext(w.workflowInfo.WorkflowExecution.RunID)
			w.cached = false
		}
		// Clear the state so other tasks waiting on the context know it should be discarded.
		w.clearState()
	} else if !w.cached {
		// Clear the state if we never cached the workflow so coroutines can be
		// exited
		w.clearState()
	}
}

func (w *workflowExecutionContextImpl) getEventHandler() *workflowExecutionEventHandlerImpl {
	if w.eventHandler == nil {
		return nil
	}
	return (*w.eventHandler).(*workflowExecutionEventHandlerImpl)
}

func (w *workflowExecutionContextImpl) completeWorkflow(result *commonpb.Payloads, err error) {
	w.isWorkflowCompleted = true
	w.result = result
	w.err = err
}

func (w *workflowExecutionContextImpl) onEviction() {
	// onEviction is run by LRU cache's removeFunc in separate goroutinue
	w.mutex.Lock()

	// Emit force eviction metrics.
	// This metrics indicates too many concurrent running workflows to fit in sticky cache.
	// Eviction on error or on workflow complete is normal and expected.
	if w.err == nil && !w.isWorkflowCompleted {
		w.wth.metricsHandler.Counter(metrics.StickyCacheTotalForcedEviction).Inc(1)
	}

	w.clearState()
	w.mutex.Unlock()
}

func (w *workflowExecutionContextImpl) IsDestroyed() bool {
	return w.getEventHandler() == nil
}

func (w *workflowExecutionContextImpl) clearState() {
	w.clearCurrentTask()
	w.isWorkflowCompleted = false
	w.result = nil
	w.err = nil
	w.previousStartedEventID = 0
	w.lastHandledEventID = 0
	w.newCommands = nil
	w.newMessages = nil

	eventHandler := w.getEventHandler()
	if eventHandler != nil {
		// Set isReplay to true to prevent user code in defer guarded by !isReplaying() from running
		eventHandler.isReplay = true
		eventHandler.Close()
		w.eventHandler = nil
	}
}

func (w *workflowExecutionContextImpl) createEventHandler() {
	w.clearState()
	eventHandler := newWorkflowExecutionEventHandler(
		w.workflowInfo,
		w.completeWorkflow,
		w.wth.logger,
		w.wth.enableLoggingInReplay,
		w.wth.metricsHandler,
		w.wth.registry,
		w.wth.dataConverter,
		w.wth.failureConverter,
		w.wth.contextPropagators,
		w.wth.deadlockDetectionTimeout,
		w.wth.capabilities,
	)

	w.eventHandler = &eventHandler
}

func resetHistory(task *workflowservice.PollWorkflowTaskQueueResponse, historyIterator HistoryIterator) (*historypb.History, error) {
	historyIterator.Reset()
	firstPageHistory, err := historyIterator.GetNextPage()
	if err != nil {
		return nil, err
	}
	task.History = firstPageHistory
	return firstPageHistory, nil
}

func (wth *workflowTaskHandlerImpl) createWorkflowContext(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowExecutionContextImpl, error) {
	h := task.History
	startedEvent := h.Events[0]
	attributes := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	if attributes == nil {
		return nil, errors.New("first history event is not WorkflowExecutionStarted")
	}
	taskQueue := attributes.TaskQueue
	if taskQueue == nil || taskQueue.Name == "" {
		return nil, errors.New("nil or empty TaskQueue in WorkflowExecutionStarted event")
	}

	runID := task.WorkflowExecution.GetRunId()
	workflowID := task.WorkflowExecution.GetWorkflowId()

	// Setup workflow Info
	var parentWorkflowExecution *WorkflowExecution
	if attributes.ParentWorkflowExecution != nil {
		parentWorkflowExecution = &WorkflowExecution{
			ID:    attributes.ParentWorkflowExecution.GetWorkflowId(),
			RunID: attributes.ParentWorkflowExecution.GetRunId(),
		}
	}

	var rootWorkflowExecution *WorkflowExecution
	if attributes.RootWorkflowExecution != nil {
		rootWorkflowExecution = &WorkflowExecution{
			ID:    attributes.RootWorkflowExecution.GetWorkflowId(),
			RunID: attributes.RootWorkflowExecution.GetRunId(),
		}
	}

	workflowInfo := &WorkflowInfo{
		WorkflowExecution: WorkflowExecution{
			ID:    workflowID,
			RunID: runID,
		},
		OriginalRunID:            attributes.OriginalExecutionRunId,
		FirstRunID:               attributes.FirstExecutionRunId,
		WorkflowType:             WorkflowType{Name: task.WorkflowType.GetName()},
		TaskQueueName:            taskQueue.GetName(),
		WorkflowExecutionTimeout: attributes.GetWorkflowExecutionTimeout().AsDuration(),
		WorkflowRunTimeout:       attributes.GetWorkflowRunTimeout().AsDuration(),
		WorkflowTaskTimeout:      attributes.GetWorkflowTaskTimeout().AsDuration(),
		Namespace:                wth.namespace,
		Attempt:                  attributes.GetAttempt(),
		WorkflowStartTime:        startedEvent.GetEventTime().AsTime(),
		lastCompletionResult:     attributes.LastCompletionResult,
		lastFailure:              attributes.ContinuedFailure,
		CronSchedule:             attributes.CronSchedule,
		ContinuedExecutionRunID:  attributes.ContinuedExecutionRunId,
		ParentWorkflowNamespace:  attributes.ParentWorkflowNamespace,
		ParentWorkflowExecution:  parentWorkflowExecution,
		RootWorkflowExecution:    rootWorkflowExecution,
		Memo:                     attributes.Memo,
		SearchAttributes:         attributes.SearchAttributes,
		RetryPolicy:              convertFromPBRetryPolicy(attributes.RetryPolicy),
		// Use the original execution run ID from the start event as the initial seed.
		// Original execution run ID stays the same for the entire chain of workflow resets.
		// This helps us keep child workflow IDs consistent up until a reset-point is encountered.
		currentRunID: attributes.GetOriginalExecutionRunId(),
		Priority:     convertFromPBPriority(attributes.Priority),
	}

	return newWorkflowExecutionContext(workflowInfo, wth), nil
}

func (wth *workflowTaskHandlerImpl) GetOrCreateWorkflowContext(
	task *workflowservice.PollWorkflowTaskQueueResponse,
	historyIterator HistoryIterator,
) (workflowContext *workflowExecutionContextImpl, err error) {
	metricsHandler := wth.metricsHandler.WithTags(metrics.WorkflowTags(task.WorkflowType.GetName()))
	defer func() {
		if err == nil && workflowContext != nil && workflowContext.laTunnel == nil {
			workflowContext.laTunnel = wth.laTunnel
		}
		metricsHandler.Gauge(metrics.StickyCacheSize).Update(float64(wth.cache.getWorkflowCache().Size()))
	}()

	runID := task.WorkflowExecution.GetRunId()

	history := task.History
	isFullHistory := isFullHistory(history)

	workflowContext = nil
	if task.Query == nil || (task.Query != nil && !isFullHistory) {
		workflowContext = wth.cache.getWorkflowContext(runID)
	}
	// Verify the cached state is current and for the correct worker
	if workflowContext != nil {
		workflowContext.Lock()
		if task.Query != nil && !isFullHistory && wth == workflowContext.wth && !workflowContext.IsDestroyed() {
			// query task and we have a valid cached state
			metricsHandler.Counter(metrics.StickyCacheHit).Inc(1)
		} else if len(history.Events) > 0 && history.Events[0].GetEventId() == workflowContext.previousStartedEventID+1 && wth == workflowContext.wth && !workflowContext.IsDestroyed() {
			// non query task and we have a valid cached state
			metricsHandler.Counter(metrics.StickyCacheHit).Inc(1)
		} else {
			// possible another task already destroyed this context.
			if !workflowContext.IsDestroyed() {
				// non query task and cached state is missing events, we need to discard the cached state and build a new one.
				if len(history.Events) > 0 && history.Events[0].GetEventId() != workflowContext.previousStartedEventID+1 {
					wth.logger.Debug("Cached state staled, new task has unexpected events",
						tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
						tagRunID, task.WorkflowExecution.GetRunId(),
						tagAttempt, task.Attempt,
						tagCachedPreviousStartedEventID, workflowContext.previousStartedEventID,
						tagTaskFirstEventID, task.History.Events[0].GetEventId(),
						tagTaskStartedEventID, task.GetStartedEventId(),
						tagPreviousStartedEventID, task.GetPreviousStartedEventId(),
					)
				} else {
					wth.logger.Debug("Cached state started on different worker, creating new context")
				}
				wth.cache.removeWorkflowContext(runID)
				workflowContext.clearState()
			}
			workflowContext.Unlock(err)
			workflowContext = nil
		}
	}
	// If the workflow was not cached or the cache was stale.
	if workflowContext == nil {
		if !isFullHistory {
			// we are getting partial history task, but cached state was already evicted.
			// we need to reset history so we get events from beginning to replay/rebuild the state
			metricsHandler.Counter(metrics.StickyCacheMiss).Inc(1)
			if _, err = resetHistory(task, historyIterator); err != nil {
				return
			}
		}

		if workflowContext, err = wth.createWorkflowContext(task); err != nil {
			return
		}

		if wth.cache.MaxWorkflowCacheSize() > 0 && task.Query == nil {
			workflowContext, _ = wth.cache.putWorkflowContext(runID, workflowContext)
			workflowContext.Lock()
			workflowContext.cached = true
		} else {
			workflowContext.Lock()
		}
	}

	err = workflowContext.resetStateIfDestroyed(task, historyIterator)
	if err != nil {
		workflowContext.Unlock(err)
	}

	return
}

func isFullHistory(history *historypb.History) bool {
	if len(history.Events) == 0 || history.Events[0].GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		return false
	}
	return true
}

func (w *workflowExecutionContextImpl) resetStateIfDestroyed(task *workflowservice.PollWorkflowTaskQueueResponse, historyIterator HistoryIterator) error {
	// It is possible that 2 threads (one for workflow task and one for query task) that both are getting this same
	// cached workflowContext. If one task finished with err, it would destroy the cached state. In that case, the
	// second task needs to reset the cache state and start from beginning of the history.
	if w.IsDestroyed() {
		w.createEventHandler()
		// reset history events if necessary
		if !isFullHistory(task.History) {
			if _, err := resetHistory(task, historyIterator); err != nil {
				return err
			}
		}
		if w.workflowInfo != nil {
			// Reset the search attributes and memos from the WorkflowExecutionStartedEvent.
			// The search attributes and memo may have been modified by calls like UpsertMemo
			// or UpsertSearchAttributes. They must be reset to avoid non determinism on replay.
			h := task.History
			startedEvent := h.Events[0]
			attributes := startedEvent.GetWorkflowExecutionStartedEventAttributes()
			if attributes == nil {
				return errors.New("first history event is not WorkflowExecutionStarted")
			}
			w.workflowInfo.SearchAttributes = attributes.SearchAttributes
			w.workflowInfo.Memo = attributes.Memo
		}
	}
	return nil
}

// ProcessWorkflowTask processes all the events of the workflow task.
func (wth *workflowTaskHandlerImpl) ProcessWorkflowTask(
	workflowTask *workflowTask,
	workflowContext *workflowExecutionContextImpl,
	heartbeatFunc workflowTaskHeartbeatFunc,
) (taskCompletion *workflowTaskCompletion, errRet error) {
	// workflowTask 是 SDK 内部包装；workflowTask.task 才是 server 返回的 PollWorkflowTaskQueueResponse。
	// 两者任意一个为空都说明调用方传入了非法 task，不能继续处理。
	if workflowTask == nil || workflowTask.task == nil {
		return nil, errors.New("nil workflow task provided")
	}
	// 取出 server 返回的 Workflow Task response，后面简称 task。
	task := workflowTask.task
	// Query Task 可能走 sticky cache 路径，server 返回的 History 可能为空但 task.Query 非空。
	// 为了后面统一访问 task.History.Events，这里把 nil History 规范化为空 History。
	if task.History == nil || len(task.History.Events) == 0 {
		task.History = &historypb.History{
			Events: []*historypb.HistoryEvent{},
		}
	}
	// 普通 Workflow Task 必须带 history。
	// 如果没有 Query，也没有任何 history event，SDK 无法 replay/执行 workflow。
	if task.Query == nil && len(task.History.Events) == 0 {
		return nil, errors.New("nil or empty history")
	}

	// task.Query 是 legacy single query 字段；task.Queries 是 buffered queries map。
	// 一个 workflow task 不能同时既是 legacy query task，又携带 buffered queries。
	if task.Query != nil && len(task.Queries) != 0 {
		return nil, errors.New("invalid query workflow task")
	}

	// 取 runID/workflowID 只是为了日志；真正处理用的是 task 本身。
	runID := task.WorkflowExecution.GetRunId()
	workflowID := task.WorkflowExecution.GetWorkflowId()
	traceLog(func() {
		// 记录当前开始处理的 workflow task 信息，包括 workflow type/id/run/attempt/sticky replay 边界。
		wth.logger.Debug("Processing new workflow task.",
			tagWorkflowType, task.WorkflowType.GetName(),
			tagWorkflowID, workflowID,
			tagRunID, runID,
			tagAttempt, task.Attempt,
			tagPreviousStartedEventID, task.GetPreviousStartedEventId())
	})

	var (
		// response 是当前 workflow task 处理完成后要发回 server 的 completion 包装。
		// 里面可能是 RespondWorkflowTaskCompleted、RespondWorkflowTaskFailed 或 RespondQueryTaskCompleted。
		response *workflowTaskCompletion
		// err 是本轮 workflow task 处理错误；最后赋给 errRet 返回给上层。
		err error
		// heartbeatTimer 用于 local activity 等待期间的 workflow task heartbeat/force complete 计时。
		heartbeatTimer *time.Timer
	)

	// 函数退出时停止 heartbeatTimer，避免 timer 泄漏。
	defer func() {
		if heartbeatTimer != nil {
			heartbeatTimer.Stop()
		}
	}()

	// 外层循环：处理当前 workflow task。
	// 如果 local activity 等待太久触发 heartbeatFunc，server 可能返回新的 workflow task；
	// 这时会更新 workflowTask 并 continue 回来继续处理。
processWorkflowLoop:
	for {
		// 记录本轮处理开始时间，用于计算 workflow task heartbeat 触发时间。
		startTime := time.Now()
		// 这里进入真正的 workflow execution context 处理：
		// - replay history；
		// - 执行 workflow 代码/query handler；
		// - 生成 completion 或发现还要等待 local activity。
		response, err = workflowContext.ProcessWorkflowTask(workflowTask)
		// err == nil && response == nil 表示 workflow task 暂时还不能完成，
		// 常见原因是 workflow 正在等待 local activity 结果。
		if err == nil && response == nil {
			// 等 local activity 的循环。
			// 期间可能收到 local activity retry、local activity result，或者达到 heartbeat 时间。
		waitLocalActivityLoop:
			for {
				// 为了避免 Workflow Task 接近超时，SDK 不等到完整 WorkflowTaskTimeout。
				// 它按 ratioToForceCompleteWorkflowTaskComplete 比例提前触发 force complete/heartbeat。
				deadlineToTrigger := time.Duration(float32(ratioToForceCompleteWorkflowTaskComplete) * float32(workflowContext.workflowInfo.WorkflowTaskTimeout))
				// 计算距离本轮 startTime + 提前触发时间还有多久。
				delayDuration := time.Until(startTime.Add(deadlineToTrigger))

				// heartbeatLoop 用来等待 timer、local activity retry/result 三类事件。
			heartbeatLoop:
				for {
					// delayDuration <= 0 表示已经到达 force complete/heartbeat 时间。
					if delayDuration <= 0 {
						// 旧 timer 如果存在，先停掉并清空。
						if heartbeatTimer != nil {
							heartbeatTimer.Stop()
							heartbeatTimer = nil
						}

						// For non-graceful shutdown, the LA worker stops before this function, so there
						// is no need to continue heartbeating. Instead, we can exit early, giving up
						// the slot this function takes, a little sooner.
						// 如果 local activity tunnel 已经停止，说明 worker 正在关闭。
						// 此时没必要继续 heartbeat，直接返回释放当前 task slot。
						select {
						case <-workflowContext.laTunnel.stopCh:
							// stopCh closed means worker is shutting down and there's
							// no need for LA heartbeat
							return
						default:
							// force complete, call the workflow task heartbeat function
							// 还没关停：强制 complete 当前 workflow task。
							// CompleteWorkflowTask(..., false) 会构造当前已有 commands/markers 的 completion；
							// heartbeatFunc 负责把它发给 server，如果 server 顺手返回新 WFT，则返回新的 workflowTask。
							workflowTask, err = heartbeatFunc(
								workflowContext.CompleteWorkflowTask(workflowTask, false),
								startTime,
							)
							if err != nil {
								// heartbeat/force complete 失败，包装成 workflowTaskHeartbeatError。
								// 上层 processWorkflowTask 会识别这个错误，避免再走普通 completion 回复。
								errRet = &workflowTaskHeartbeatError{Message: fmt.Sprintf("error sending workflow task heartbeat %v", err)}
								return
							}
							// heartbeat 成功但 server 没有返回新 workflow task，当前处理结束。
							if workflowTask == nil {
								return
							}
						}

						// server 返回了新 workflow task，回到外层 processWorkflowLoop 处理这个新 task。
						continue processWorkflowLoop
					}

					// 如果还没到 heartbeat 时间，创建 timer 等待。
					if heartbeatTimer == nil {
						heartbeatTimer = time.NewTimer(delayDuration)
					}

					// 等待三类事件：heartbeat timer、local activity retry、local activity result。
					select {
					case <-heartbeatTimer.C:
						// timer 到期，把 delayDuration 置 0，下一轮 heartbeatLoop 会进入 force complete 分支。
						delayDuration = 0
						continue heartbeatLoop

					case laRetry := <-workflowTask.laRetryCh:
						// 收到一个需要重试的 local activity task。
						eventHandler := workflowContext.getEventHandler()

						// if workflow task heartbeat failed, the workflow execution context will be cleared and eventHandler will be nil
						// 如果 eventHandler 已经被清空，说明 workflow context 已不可用，退出处理循环。
						if eventHandler == nil {
							break processWorkflowLoop
						}

						// 如果这个 local activity 已经不在 pending 集合里，说明当前 workflow task 状态已经变化，
						// 不再继续重试它，退出处理循环。
						if _, ok := eventHandler.pendingLaTasks[laRetry.activityID]; !ok {
							break processWorkflowLoop
						}

						// 增加 local activity retry attempt。
						laRetry.attempt++

						// 把 local activity retry task 重新送进 local activity tunnel。
						// 如果发送失败，回滚 attempt，避免错误地消耗重试次数。
						if !wth.laTunnel.sendTask(laRetry) {
							laRetry.attempt--
						}

					case lar := <-workflowTask.laResultCh:
						// local activity result ready
						// 收到 local activity 结果，把结果应用到 workflow context。
						// 这可能让 workflow 继续推进并生成 completion，也可能还要等其它 local activity。
						response, err = workflowContext.ProcessLocalActivityResult(workflowTask, lar)
						if err == nil && response == nil {
							// workflow task is not done yet, still waiting for more local activities
							// 这个 local activity 结果还不足以完成当前 WFT，继续等更多 local activity。
							continue waitLocalActivityLoop
						}
						// local activity 结果让当前 workflow task 可以结束，跳出全部处理循环。
						break processWorkflowLoop
					}
				}
			}
		} else {
			// workflowContext.ProcessWorkflowTask 已经得到了 completion 或错误。
			// 不需要等 local activity，直接结束处理循环。
			break processWorkflowLoop
		}
	}
	// 把本函数内部 err 赋给命名返回值 errRet。
	errRet = err
	// 把处理得到的 response 赋给命名返回值 taskCompletion。
	taskCompletion = response
	// 返回给 workflowTaskProcessor.processWorkflowTask，由它负责把 completion 发回 server。
	return
}

func (w *workflowExecutionContextImpl) ProcessWorkflowTask(workflowTask *workflowTask) (*workflowTaskCompletion, error) {
	// 从 SDK 内部 workflowTask 包装中取出 server 返回的 PollWorkflowTaskQueueResponse。
	// 这个 task 代表“这一次 worker poll 到的 Workflow Task”，里面带 TaskToken、WorkflowType、History、Query 等信息。
	task := workflowTask.task
	// historyIterator 用来在当前 PollWorkflowTaskQueueResponse 只带第一页 history 时，继续按 NextPageToken 拉后续 history。
	// 也就是说 replay 不一定只靠 task.History.Events 这一页；需要更多 history 时会通过 iterator 继续取。
	historyIterator := workflowTask.historyIterator
	// 如果当前 workflow execution 在本地 sticky cache 里有旧 context，要先检查它是不是还能接着用。
	// 例如本地以为上次处理到 event 10，但这次 server 给的第一条 history 不是 11，说明缓存状态和 server history 对不上。
	// 这种情况下 ResetIfStale 会清理/重置本地状态，避免用错误的内存状态继续跑 workflow。
	if err := w.ResetIfStale(task, historyIterator); err != nil {
		return nil, err
	}
	// 把当前 server task 记录到 workflowExecutionContext。
	// 普通 Workflow Task 会更新 previousStartedEventID；Query Task 不更新，因为 Query 不推进 history。
	w.SetCurrentTask(task)

	// eventHandler 是真正把 history event 应用到 workflow 状态机/用户 workflow goroutine 的对象。
	// 它持有 commandsHelper、pending local activity、query/update handler 等运行时状态。
	eventHandler := w.getEventHandler()
	// newHistory 会把本次 Workflow Task 的 history 整理成 SDK 更方便处理的任务片段。
	// 它会区分 replay 事件、当前 WFT 新事件、markers、accepted/admitted update messages 等。
	// lastHandledEventID 是 sticky cache 下的 replay 起点：已经处理过的 event 不需要重复应用。
	reorderedHistory := newHistory(w.lastHandledEventID, workflowTask, eventHandler)
	defer func() {
		// 本次处理结束后，把“已经看过的最后一个 eventID”保存回 context。
		// 即使本次处理失败也更新：因为失败会导致 cache 被丢弃，下次会从完整 history 重新 replay，
		// 这里记录 lastHandledEventID 不会让错误状态被继续复用。
		w.lastHandledEventID = reorderedHistory.lastHandledEventID
	}()
	// replayOutbox 保存 replay 阶段产生的 outgoing protocol messages。
	// 后面会和 history 中真实 command/message 对比，用来检查 workflow 代码是否仍然 deterministic。
	var replayOutbox []outboxEntry
	// replayCommands 保存 replay 阶段用户代码重新生成的 commands。
	// 例如历史里曾经 ScheduleActivity，replay 时代码也必须生成同样的 ScheduleActivity command。
	var replayCommands []*commandpb.Command
	// respondEvents 保存 history 里已经存在的、由之前 commands 产生的事件。
	// 它是 replayCommands 的“答案”，后面 matchReplayWithHistory 会拿两者做确定性校验。
	var respondEvents []*historypb.HistoryEvent
	// partialHistory 表示当前处理的是 replayer 中的部分 history，最后一个事件停在 WorkflowTaskStarted。
	// 这种情况下不能用不完整 command 集合做完整确定性判断。
	var partialHistory bool

	// 取出 server 在 PollWorkflowTaskQueueResponse 上直接携带的 protocol messages。
	// 非 replay 的当前 WFT 可能会用这些 messages；replay 时通常从 history event 里还原 messages。
	taskMessages := workflowTask.task.GetMessages()
	// Query Task 或 partial history 不适合做完整 replay command 校验，因此 skipReplayCheck 会为 true。
	skipReplayCheck := w.skipReplayCheck()
	// replay namespace 是 SDK replayer/test replay 的特殊 namespace；在这里即使 workflow 已完成也可能强制继续校验。
	isInReplayer := IsReplayNamespace(w.wth.namespace)
	// shouldForceReplayCheck 决定 workflow 已经完成后是否还要继续 replay/check。
	// 正常 worker 看到 workflow complete 后可以停；replayer 为了验证整段 history，通常要继续。
	shouldForceReplayCheck := func() bool {
		// 如果 workflow panic 过，就不强行继续 replay check，避免把旧 history 以新 panic 路径误判。
		_, wfPanicked := w.err.(*workflowPanicError)
		return !wfPanicked && isInReplayer
	}

	// 记录当前 replayCommands 的下标。
	// 处理 partial history 时，如果最后发现 history 不完整，会回退到这个位置，避免把不完整 WFT 的 commands 算进去。
	curReplayCmdsIndex := -1

	// 为当前 workflow type 创建 metrics handler，用来记录 replay latency 等指标。
	metricsHandler := w.wth.metricsHandler.WithTags(metrics.WorkflowTags(task.WorkflowType.GetName()))
	// start 是本次 ProcessWorkflowTask 开始时间，后面用于计算 replay 花了多久。
	start := time.Now()
	// metricsTimer 只记录一次 replay latency；一旦遇到第一个非 replay event，就记录并置 nil。
	metricsTimer := metricsHandler.Timer(metrics.WorkflowTaskReplayLatency)

	// 清空“本次 WFT 内 Local Activity 非首次 attempt 次数”的统计。
	// CompleteWorkflowTask 时会把这些统计放进 MeteringMetadata。
	eventHandler.ResetLAWFTAttemptCounts()
	// 标记 SDK flags 已经被发送/记录过，后面只采集本次新产生的 flags。
	eventHandler.sdkFlags.markSDKFlagsSent()

	// 把当前 worker build id 写进 workflow info。
	// replay 历史事件时如果 history 带了 build id，后面会按历史值覆盖。
	w.workflowInfo.currentTaskBuildID = w.wth.workerBuildID
ProcessEvents:
	// 外层循环逐段处理 reorderedHistory。
	// 一段 nextTask 通常对应 replay 历史中的一个 WFT 区间，或者当前最新 WFT 的事件集合。
	for {
		// 取下一段需要处理的 history/messages/markers。
		// 如果当前 response 的 history 不够，reorderedHistory 可能通过 historyIterator 继续拉下一页。
		nextTask, err := reorderedHistory.nextTask()
		if err != nil {
			return nil, err
		}
		// reorderedEvents 是这一段真正要按顺序喂给 eventHandler 的 history events。
		reorderedEvents := nextTask.events
		// markers 是从这一段中提前拆出来的 MarkerRecorded events。
		// Local Activity marker 要特殊处理，因为它需要在 WFT started 之后应用。
		markers := nextTask.markers
		// historyMessages 是从 history events 中还原出来的 protocol messages，例如 update 相关消息。
		historyMessages := nextTask.acceptedMsgs
		// flags 是历史里记录的 SDK flags，用来让 replay 按当时 SDK 语义解释历史。
		flags := nextTask.flags
		// binaryChecksum 是这段历史关联的 worker build/checksum。
		binaryChecksum := nextTask.binaryChecksum
		// nextTaskBuildId 是 worker versioning/build id 信息，replay 时要写回 workflowInfo。
		nextTaskBuildId := nextTask.buildID
		// admittedUpdates 是当前 WFT 新带入的 update messages，可能需要替换 accepted event 里合成出的消息。
		admittedUpdates := nextTask.admittedMsgs

		// 在 replay 工具里，如果最后一段 history 只到 WorkflowTaskStarted，
		// 说明这是一段“不完整 Workflow Execution history”：WFT started 之后还没有 completed/failed 等结果事件。
		// 这种情况下停止处理，并把 partialHistory 标出来，后面避免错误地做完整确定性校验。
		isLastWFTForPartialWFE := len(reorderedEvents) > 0 &&
			reorderedEvents[len(reorderedEvents)-1].EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED &&
			len(reorderedHistory.next) == 0 &&
			isInReplayer
		if isLastWFTForPartialWFE {
			partialHistory = true
			break ProcessEvents
		}

		// 判断这一段是不是 replay 历史。
		// replay 段必须用 history 中还原出来的 messages；当前新 WFT 段才使用 task response 直接带的 messages。
		isReplay := len(reorderedEvents) > 0 && reorderedHistory.IsReplayEvent(reorderedEvents[len(reorderedEvents)-1])
		// msgs 是按 eventID 建索引后的 messages 集合。
		// 后面会在 ProcessEvent 前后按 eventID 把对应 message 投递给 eventHandler。
		var msgs *eventMsgIndex
		if isReplay {
			// 把 admitted update messages 先按 protocol instance ID 建 map，方便替换。
			admittedUpdatesByID := make(map[string]*protocolpb.Message, len(admittedUpdates))
			for _, admittedUpdate := range admittedUpdates {
				admittedUpdatesByID[admittedUpdate.GetProtocolInstanceId()] = admittedUpdate
			}
			// replay 时，某些 update message 可能既能从 accepted event 合成，也可能由 admitted event 提供。
			// 如果 admitted 版本存在，就用 admitted 版本替换，保证 handler 看到的是 server 对当前 WFT 接纳的消息。
			for i, msg := range historyMessages {
				if admittedUpdate, ok := admittedUpdatesByID[msg.GetProtocolInstanceId()]; ok {
					historyMessages[i] = admittedUpdate
				}
				// 到这里每个 message 都必须有 body；没有 body 说明 history/message 构造不完整，不能继续驱动 workflow。
				if historyMessages[i].Body == nil {
					return nil, fmt.Errorf("missing body in message for update ID %v", msg.GetProtocolInstanceId())
				}
			}
			// 用 history messages 建索引；replay 必须以 history 为准。
			msgs = indexMessagesByEventID(historyMessages)

			// replay 历史时，把历史里记录的 SDK 版本/名称恢复到 eventHandler。
			// 这会影响一些兼容逻辑，保证按当时写 history 的 SDK 行为解释事件。
			eventHandler.sdkVersion = nextTask.sdkVersion
			eventHandler.sdkName = nextTask.sdkName
		} else {
			// 当前非 replay 段使用 poll response 上携带的 messages，并补上 admitted updates。
			taskMessages = append(taskMessages, admittedUpdates...)
			// 按 eventID 建索引，后面 ProcessEvent 前后会把 message 投递进去。
			msgs = indexMessagesByEventID(taskMessages)
			// 当前 WFT 的 taskMessages 已经转进 msgs，清空避免下一段重复处理。
			taskMessages = []*protocolpb.Message{}
			// 如果当前 eventHandler 记录的 SDK version 不是当前 SDKVersion，标记发生更新。
			if eventHandler.sdkVersion != SDKVersion {
				eventHandler.sdkVersionUpdated = true
				eventHandler.sdkVersion = SDKVersion
			}
			// 如果 SDK name 发生变化，也标记更新。
			if eventHandler.sdkName != SDKName {
				eventHandler.sdkNameUpdated = true
				eventHandler.sdkName = SDKName
			}
		}

		// 把这段 history 里的 SDK flags 写入 eventHandler，后续处理事件/commands 时会读取这些兼容 flags。
		eventHandler.sdkFlags.set(flags...)
		// 没有事件可处理，说明 history 已经读完或当前段为空，跳出事件处理循环。
		if len(reorderedEvents) == 0 {
			break ProcessEvents
		}
		// replayCommands 会在每段 replay 后更新。
		// 这里提前记录更新前的位置，用于 partial history 回退。
		curReplayCmdsIndex = len(replayCommands)

		// 恢复 workflowInfo.BinaryChecksum。
		// 老 history 可能没有 binaryChecksum，此时用当前 workerBuildID 兜底。
		if binaryChecksum == "" {
			w.workflowInfo.BinaryChecksum = w.wth.workerBuildID
		} else {
			w.workflowInfo.BinaryChecksum = binaryChecksum
		}
		// replay 历史时，如果这段 history 带了 build id，就恢复 currentTaskBuildID。
		if isReplay && nextTaskBuildId != nil {
			w.workflowInfo.currentTaskBuildID = *nextTaskBuildId
		}
		// 每段 WFT 开始前清空 mutable side effect marker 的“已记录”集合。
		// 这样 replay 当前段时可以重新判断哪些 marker 已经由 history 提供，哪些需要生成 command。
		eventHandler.mutableSideEffectsRecorded = nil
		// 先处理非 Local Activity marker。
		// 这些 marker 属于当前 WFT 产生的事件，但 Local Activity marker 需要等 WorkflowTaskStarted event 应用后再处理。
		for _, m := range markers {
			if m.GetMarkerRecordedEventAttributes().GetMarkerName() != localActivityMarkerName {
				// 非 Local Activity marker，例如 SideEffect/MutableSideEffect/Version marker，直接按 replay event 应用。
				err := eventHandler.ProcessEvent(m, true, false)
				if err != nil {
					return nil, err
				}
				// 如果 marker 让 workflow 进入完成状态，正常 worker 可以停止继续处理；
				// replayer 可能仍要继续检查后续 history。
				if w.isWorkflowCompleted && !shouldForceReplayCheck() {
					break ProcessEvents
				}
			}
		}

		// 按 eventID 顺序处理这一段 history events。
		// ProcessEvent 是驱动 workflow 的核心：WorkflowTaskStarted 会启动/恢复 workflow goroutine，
		// ActivityTaskCompleted 会唤醒 Future.Get，Signal event 会投递 signal channel，等等。
		for i, event := range reorderedEvents {
			// 单个 event 是否属于 replay 历史。
			// 同一段里通常一致，但这里逐 event 判断，适配 reorderedHistory 的边界情况。
			isInReplay := reorderedHistory.IsReplayEvent(event)
			// 一旦遇到第一个非 replay event，说明 replay 阶段结束、进入当前新事件处理阶段。
			// 此时记录 WorkflowTaskReplayLatency。
			if !isInReplay && metricsTimer != nil {
				metricsTimer.Record(time.Since(start))
				metricsTimer = nil
			}

			// isLast 表示这是当前非 replay 段的最后一个 event。
			// eventHandler 可能用它判断当前 WFT 是否已经到达可继续执行 workflow 代码的位置。
			isLast := !isInReplay && i == len(reorderedEvents)-1
			// 如果当前事件是由 command 产生的 history event，收集起来做 replay 确定性校验。
			// 例如 ActivityTaskScheduled/TimerStarted 等会对应 replayCommands 中的 command。
			if !skipReplayCheck && isCommandEvent(event.GetEventType()) {
				respondEvents = append(respondEvents, event)
			}

			// preload marker 已经在 markers 分支里单独处理，这里跳过，避免重复应用。
			if isPreloadMarkerEvent(event) {
				continue
			}

			// 测试/故障注入用的 pressure point。
			// 生产正常路径一般不会有错误；如果这里返回错误，就模拟在指定 event 处理点失败。
			err := w.wth.executeAnyPressurePoints(event, isInReplay)
			if err != nil {
				return nil, err
			}

			// 先处理 eventID 小于当前 event 的 messages。
			// 原因：不是所有承载 message 的 history event 都会走这个 loop；
			// 有些 message 可能挂在 WorkflowTaskScheduled 之类被重排/跳过的 event 上。
			// 所以在每个 event 前先补投递 <= eventID-1 的 messages，保证 message 的可见顺序正确。
			for _, msg := range msgs.takeLTE(event.GetEventId() - 1) {
				err := eventHandler.ProcessMessage(msg, isInReplay, isLast)
				if err != nil {
					return nil, err
				}
				// message 处理可能让 workflow 完成，例如 update handler 返回并触发完成路径。
				if w.isWorkflowCompleted && !shouldForceReplayCheck() {
					break ProcessEvents
				}
			}

			// 应用当前 history event。
			// 这是 replay/推进 workflow 的核心调用：eventHandler 根据 event 类型更新 command 状态机、
			// 唤醒 workflow goroutine、恢复 Future/Channel/Timer/Activity/Signal 等 SDK 对象。
			err = eventHandler.ProcessEvent(event, isInReplay, isLast)
			if err != nil {
				return nil, err
			}
			// 如果当前 event 让 workflow 已经完成，正常 worker 不需要继续处理后续事件。
			if w.isWorkflowCompleted && !shouldForceReplayCheck() {
				break ProcessEvents
			}

			// 再处理 eventID 小于等于当前 event 的 messages。
			// 这补上“应该在当前 event 之后可见”的 messages，和前面的 takeLTE(eventID-1) 配合保证顺序。
			for _, msg := range msgs.takeLTE(event.GetEventId()) {
				err := eventHandler.ProcessMessage(msg, isInReplay, isLast)
				if err != nil {
					return nil, err
				}
				// message 处理后再次检查 workflow 是否已经完成。
				if w.isWorkflowCompleted && !shouldForceReplayCheck() {
					break ProcessEvents
				}
			}
		}

		// 最后再应用 Local Activity marker。
		// Local Activity marker 记录的是本地 Activity 的结果/失败/attempt/backoff 等。
		// 它必须在 WorkflowTaskStarted 之后处理，因为处理 marker 会回调 workflow future，
		// 让 workflow 从 ExecuteLocalActivity(...).Get(...) 继续往下跑。
		for _, m := range markers {
			if m.GetMarkerRecordedEventAttributes().GetMarkerName() == localActivityMarkerName {
				// 这里传 isReplay=true，因为 marker 已经在 history 里；
				// SDK 应该读取 marker 中的结果，而不是重新执行 Local Activity。
				err := eventHandler.ProcessEvent(m, true, false)
				if err != nil {
					return nil, err
				}
				// Local Activity marker 可能让 workflow 继续执行并最终完成。
				if w.isWorkflowCompleted && !shouldForceReplayCheck() {
					break ProcessEvents
				}
			}
		}
		// 如果这一段是 replay 历史，就把 replay 过程中重新生成的 commands/outbox 收集起来。
		// 后面用它们和 history 里真实发生过的 respondEvents 做确定性匹配。
		if isReplay {
			// getCommands(true) 表示取出 replay 阶段 command state machine 认为应该产生的 commands。
			eventCommands := eventHandler.commandsHelper.getCommands(true)
			if !skipReplayCheck {
				// Query/partial history 会跳过 replay check；普通 replay 才收集这些数据。
				replayCommands = append(replayCommands, eventCommands...)
				replayOutbox = append(replayOutbox, eventHandler.outbox...)
			}
			// replay outbox 已经被收集，清空 eventHandler 的临时 outbox，避免污染后续当前 WFT 的真实 outgoing messages。
			eventHandler.outbox = nil
		}
	}

	// 如果是 partial history，回退 replayCommands 到当前不完整 WFT 之前。
	// 因为不完整 WFT 还没有对应的 completed/failed event，不能用它生成的 commands 做最终确定性校验。
	if partialHistory && curReplayCmdsIndex != -1 {
		replayCommands = replayCommands[:curReplayCmdsIndex]
	}

	// 如果整个循环里没有遇到非 replay event，也要在结束时记录 replay latency。
	if metricsTimer != nil {
		metricsTimer.Record(time.Since(start))
		metricsTimer = nil
	}

	// workflowError 用于保存 workflow 级错误，特别是 non-deterministic error。
	//
	// 非确定性通常来自两类情况：
	// 1. replay 生成的 commands 和 history 里已经发生的 events 对不上。
	//    例如老代码曾经 ExecuteActivity(A)，history 里有 ActivityTaskScheduled(A)，
	//    新代码改成 ExecuteActivity(B)，replayCommands 就会变成 ScheduleActivity(B)，从而不匹配。
	// 2. replay history event 时 command state machine 发现非法状态转换。
	//    例如 history 里有 ActivityTaskCompleted，但新代码已经删除了当初 schedule activity 的逻辑，
	//    SDK 没有对应 command 状态可以接这个 completed event。
	var workflowError error
	// 只有在允许 replay check，且 workflow 没完成或 replayer 强制继续检查时，才做 command/history 匹配。
	if !skipReplayCheck && (!w.isWorkflowCompleted || shouldForceReplayCheck()) {
		// 对比 replayCommands 与 respondEvents/replayOutbox。
		// 如果不匹配，说明当前 workflow 代码无法 deterministic 地重放已有 history。
		if err := matchReplayWithHistory(replayCommands, respondEvents, replayOutbox, w.getEventHandler().sdkFlags); err != nil {
			workflowError = err
			// 把错误也写到 workflow context，后续 applyWorkflowPanicPolicy/completeWorkflow 会按策略处理。
			w.err = err
		}
	}

	// 根据 workflowError/w.err/query/local activity 等状态生成本次 WFT 的 completion。
	// 可能返回：
	// - RespondWorkflowTaskCompletedRequest：带 commands，正常回复 server。
	// - RespondWorkflowTaskFailedRequest：workflow task 失败。
	// - RespondQueryTaskCompletedRequest：Query Task 只返回 query result。
	// - nil, nil：例如还有 pending local activity，需要外层继续等。
	return w.applyWorkflowPanicPolicy(workflowTask, workflowError)
}

func (w *workflowExecutionContextImpl) ProcessLocalActivityResult(workflowTask *workflowTask, lar *localActivityResult) (*workflowTaskCompletion, error) {
	if lar.err != nil && w.retryLocalActivity(lar) {
		return nil, nil // nothing to do here as we are retrying...
	}

	return w.applyWorkflowPanicPolicy(workflowTask, w.getEventHandler().ProcessLocalActivityResult(lar))
}

func (w *workflowExecutionContextImpl) applyWorkflowPanicPolicy(workflowTask *workflowTask, workflowError error) (*workflowTaskCompletion, error) {
	task := workflowTask.task

	if workflowError == nil && w.err != nil {
		if panicErr, ok := w.err.(*workflowPanicError); ok {
			workflowError = panicErr
		}
	}

	if workflowError != nil {
		if panicErr, ok := w.err.(*workflowPanicError); ok {
			w.wth.logger.Error("Workflow panic",
				tagWorkflowType, task.WorkflowType.GetName(),
				tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
				tagRunID, task.WorkflowExecution.GetRunId(),
				tagAttempt, task.Attempt,
				tagError, workflowError,
				tagStackTrace, panicErr.StackTrace())
		} else {
			w.wth.logger.Error("Workflow panic",
				tagWorkflowType, task.WorkflowType.GetName(),
				tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
				tagRunID, task.WorkflowExecution.GetRunId(),
				tagAttempt, task.Attempt,
				tagError, workflowError)
		}

		switch w.wth.workflowPanicPolicy {
		case FailWorkflow:
			// complete workflow with custom error will fail the workflow
			w.getEventHandler().Complete(nil, NewApplicationError(
				"Workflow failed on panic due to FailWorkflow workflow panic policy",
				"", false, workflowError))
		case BlockWorkflow:
			// return error here will be convert to WorkflowTaskFailed for the first time, and ignored for subsequent
			// attempts which will cause WorkflowTaskTimeout and server will retry forever until issue got fixed or
			// workflow timeout.
			return nil, workflowError
		default:
			panic("unknown mismatched workflow history policy.")
		}
	}

	return w.CompleteWorkflowTask(workflowTask, true), nil
}

func (w *workflowExecutionContextImpl) retryLocalActivity(lar *localActivityResult) bool {
	if lar.task.retryPolicy == nil || lar.err == nil || IsCanceledError(lar.err) {
		return false
	}

	retryBackoff := getRetryBackoff(lar, time.Now())
	if retryBackoff > 0 && retryBackoff <= w.workflowInfo.WorkflowTaskTimeout {
		// we need a local retry
		time.AfterFunc(retryBackoff, func() {
			// Send retry signal
			select {
			case lar.task.workflowTask.laRetryCh <- lar.task:
			case <-lar.task.workflowTask.doneCh:
				// Task is already done. Abort retrying.
			}
		})
		return true
	}
	// Backoff could be large and potentially much larger than WorkflowTaskTimeout. We cannot just sleep locally for
	// retry. Because it will delay the local activity from complete which keeps the workflow task open. In order to
	// keep workflow task open, we have to keep "heartbeating" current workflow task.
	// In that case, it is more efficient to create a server timer with backoff duration and retry when that backoff
	// timer fires. So here we will return false to indicate we don't need local retry anymore. However, we have to
	// store the current attempt and backoff to the same LocalActivityResultMarker so the replay can do the right thing.
	// The backoff timer will be created by workflow.ExecuteLocalActivity().
	lar.backoff = retryBackoff

	return false
}

func getRetryBackoff(lar *localActivityResult, now time.Time) time.Duration {
	return getRetryBackoffWithNowTime(lar.task.retryPolicy, lar.task.attempt, lar.err, now, lar.task.expireTime)
}

func getRetryBackoffWithNowTime(p *RetryPolicy, attempt int32, err error, now, expireTime time.Time) time.Duration {
	if !IsRetryable(err, p.NonRetryableErrorTypes) {
		return noRetryBackoff
	}

	if p.MaximumAttempts > 0 && attempt >= p.MaximumAttempts {
		return noRetryBackoff // max attempt reached
	}

	var backoffInterval time.Duration
	// Extract backoff interval from error if it is a retryable error.
	// Not using errors.As() since we don't want to explore the whole error chain.
	if applicationErr, ok := err.(*ApplicationError); ok {
		backoffInterval = applicationErr.nextRetryDelay
	}
	// Calculate next backoff interval if the error did not contain the next backoff interval.
	// attempt starts from 1
	if backoffInterval == 0 {
		backoffInterval = time.Duration(float64(p.InitialInterval) * math.Pow(p.BackoffCoefficient, float64(attempt-1)))
		if backoffInterval <= 0 {
			// math.Pow() could overflow
			if p.MaximumInterval > 0 {
				backoffInterval = p.MaximumInterval
			}
		}
		if p.MaximumInterval > 0 && backoffInterval > p.MaximumInterval {
			// cap next interval to MaxInterval
			backoffInterval = p.MaximumInterval
		}
	}
	if backoffInterval <= 0 {
		return noRetryBackoff
	}

	nextScheduleTime := now.Add(backoffInterval)
	if !expireTime.IsZero() && nextScheduleTime.After(expireTime) {
		return noRetryBackoff
	}

	return backoffInterval
}

func (w *workflowExecutionContextImpl) CompleteWorkflowTask(workflowTask *workflowTask, waitLocalActivities bool) *workflowTaskCompletion {
	if w.currentWorkflowTask == nil {
		return nil
	}
	eventHandler := w.getEventHandler()

	// w.laTunnel could be nil for worker.ReplayHistory() because there is no worker started, in that case we don't
	// care about the pending local activities, and just return because the result is ignored anyway by the caller.
	if w.hasPendingLocalActivityWork() && w.laTunnel != nil {
		if len(eventHandler.unstartedLaTasks) > 0 {
			// start new local activity tasks
			unstartedLaTasks := make(map[string]struct{})
			for activityID := range eventHandler.unstartedLaTasks {
				task := eventHandler.pendingLaTasks[activityID]
				task.wc = w
				task.workflowTask = workflowTask

				task.scheduledTime = time.Now()

				if !w.laTunnel.sendTask(task) {
					unstartedLaTasks[activityID] = struct{}{}
					task.wc = nil
					task.workflowTask = nil
				}
			}
			eventHandler.unstartedLaTasks = unstartedLaTasks
		}
		// cannot complete workflow task as there are pending local activities
		if waitLocalActivities {
			return nil
		}
	}

	eventCommands := eventHandler.commandsHelper.getCommands(true)
	if len(eventCommands) > 0 {
		w.newCommands = append(w.newCommands, eventCommands...)
	}

	w.newMessages = append(w.newMessages, eventHandler.takeOutgoingMessages()...)
	eventHandler.protocols.ClearCompleted()

	completeRequest := w.wth.completeWorkflow(eventHandler, w.currentWorkflowTask, w, w.newCommands, w.newMessages, !waitLocalActivities)
	w.clearCurrentTask()

	return &completeRequest
}

func (w *workflowExecutionContextImpl) hasPendingLocalActivityWork() bool {
	eventHandler := w.getEventHandler()
	return !w.isWorkflowCompleted &&
		w.currentWorkflowTask != nil &&
		w.currentWorkflowTask.Query == nil && // don't run local activity for query task
		eventHandler != nil &&
		len(eventHandler.pendingLaTasks) > 0
}

func (w *workflowExecutionContextImpl) clearCurrentTask() {
	w.newCommands = nil
	w.newMessages = nil
	w.currentWorkflowTask = nil
}

func (w *workflowExecutionContextImpl) skipReplayCheck() bool {
	return w.currentWorkflowTask.Query != nil || !isFullHistory(w.currentWorkflowTask.History)
}

func (w *workflowExecutionContextImpl) SetCurrentTask(task *workflowservice.PollWorkflowTaskQueueResponse) {
	w.currentWorkflowTask = task
	// do not update the previousStartedEventID for query task
	if task.Query == nil {
		w.previousStartedEventID = task.GetStartedEventId()
	}
}

func (w *workflowExecutionContextImpl) SetPreviousStartedEventID(eventID int64) {
	// We must reset the last event we handled to be after the last WFT we really completed
	// + any command events (since the SDK "processed" those when it emitted the commands). This
	// is also equal to what we just processed in the speculative task, minus two, since we
	// would've just handled the most recent WFT started event, and we need to drop that & the
	// schedule event just before it.
	w.lastHandledEventID = w.lastHandledEventID - 2
	w.previousStartedEventID = eventID
}

func (w *workflowExecutionContextImpl) ResetIfStale(task *workflowservice.PollWorkflowTaskQueueResponse, historyIterator HistoryIterator) error {
	if len(task.History.Events) > 0 && task.History.Events[0].GetEventId() != w.previousStartedEventID+1 {
		w.wth.logger.Debug("Cached state staled, new task has unexpected events",
			tagWorkflowID, task.WorkflowExecution.GetWorkflowId(),
			tagRunID, task.WorkflowExecution.GetRunId(),
			tagAttempt, task.Attempt,
			tagCachedPreviousStartedEventID, w.previousStartedEventID,
			tagTaskFirstEventID, task.History.Events[0].GetEventId(),
			tagTaskStartedEventID, task.GetStartedEventId(),
			tagPreviousStartedEventID, task.GetPreviousStartedEventId(),
		)
		w.clearState()
		return w.resetStateIfDestroyed(task, historyIterator)
	}
	return nil
}

func skipDeterministicCheckForCommand(d *commandpb.Command, _ *sdkFlags) bool {
	switch d.GetCommandType() {
	case enumspb.COMMAND_TYPE_RECORD_MARKER:
		markerName := d.GetRecordMarkerCommandAttributes().GetMarkerName()
		if markerName == versionMarkerName || markerName == mutableSideEffectMarkerName {
			return true
		}
	}
	return false
}

func skipDeterministicCheckForEvent(e *historypb.HistoryEvent, sdkFlags *sdkFlags) bool {
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_MARKER_RECORDED:
		markerName := e.GetMarkerRecordedEventAttributes().GetMarkerName()
		if markerName == versionMarkerName || markerName == mutableSideEffectMarkerName {
			return true
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
		return true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
		protocolMsgCommandInUse := sdkFlags.tryUse(SDKFlagProtocolMessageCommand, false)
		return !protocolMsgCommandInUse
	}
	return false
}

// special check for upsert change version event
func skipDeterministicCheckForUpsertChangeVersion(events []*historypb.HistoryEvent, idx int) bool {
	e := events[idx]
	if e.GetEventType() == enumspb.EVENT_TYPE_MARKER_RECORDED &&
		e.GetMarkerRecordedEventAttributes().GetMarkerName() == versionMarkerName &&
		idx < len(events)-1 &&
		events[idx+1].GetEventType() == enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES {
		if _, ok := events[idx+1].GetUpsertWorkflowSearchAttributesEventAttributes().SearchAttributes.IndexedFields[TemporalChangeVersion]; ok {
			return true
		}
	}
	return false
}

func matchReplayWithHistory(
	replayCommands []*commandpb.Command,
	historyEvents []*historypb.HistoryEvent,
	msgs []outboxEntry,
	sdkFlags *sdkFlags,
) error {
	di := 0
	hi := 0
	hSize := len(historyEvents)
	dSize := len(replayCommands)
matchLoop:
	for hi < hSize || di < dSize {
		var e *historypb.HistoryEvent
		if hi < hSize {
			e = historyEvents[hi]
			if skipDeterministicCheckForUpsertChangeVersion(historyEvents, hi) {
				hi += 2
				continue matchLoop
			}
			if skipDeterministicCheckForEvent(e, sdkFlags) {
				hi++
				continue matchLoop
			}
		}

		var d *commandpb.Command
		if di < dSize {
			d = replayCommands[di]
			if skipDeterministicCheckForCommand(d, sdkFlags) {
				di++
				continue matchLoop
			}
		}

		if d == nil {
			return historyMismatchErrorf("[TMPRL1100] nondeterministic workflow: missing replay command for %s", util.HistoryEventToString(e))
		}

		if e == nil {
			return historyMismatchErrorf("[TMPRL1100] nondeterministic workflow: extra replay command for %s", util.CommandToString(d))
		}

		if !isCommandMatchEvent(d, e, msgs) {
			return historyMismatchErrorf("[TMPRL1100] nondeterministic workflow: history event is %s, replay command is %s",
				util.HistoryEventToString(e), util.CommandToString(d))
		}

		di++
		hi++
	}
	return nil
}

func lastPartOfName(name string) string {
	lastDotIdx := strings.LastIndex(name, ".")
	if lastDotIdx < 0 || lastDotIdx == len(name)-1 {
		return name
	}
	return name[lastDotIdx+1:]
}

func isCommandMatchEvent(d *commandpb.Command, e *historypb.HistoryEvent, obes []outboxEntry) bool {
	switch d.GetCommandType() {
	case enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE:
		msgid := d.GetProtocolMessageCommandAttributes().GetMessageId()
		for _, entry := range obes {
			if entry.msg.Id == msgid {
				return entry.eventPredicate(e)
			}
		}
		return false

	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		if e.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			return false
		}
		eventAttributes := e.GetActivityTaskScheduledEventAttributes()
		commandAttributes := d.GetScheduleActivityTaskCommandAttributes()

		if eventAttributes.GetActivityId() != commandAttributes.GetActivityId() ||
			lastPartOfName(eventAttributes.ActivityType.GetName()) != lastPartOfName(commandAttributes.ActivityType.GetName()) {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
		if e.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED {
			return false
		}
		commandAttributes := d.GetRequestCancelActivityTaskCommandAttributes()
		eventAttributes := e.GetActivityTaskCancelRequestedEventAttributes()
		if eventAttributes.GetScheduledEventId() != commandAttributes.GetScheduledEventId() {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_START_TIMER:
		if e.GetEventType() != enumspb.EVENT_TYPE_TIMER_STARTED {
			return false
		}
		eventAttributes := e.GetTimerStartedEventAttributes()
		commandAttributes := d.GetStartTimerCommandAttributes()

		if eventAttributes.GetTimerId() != commandAttributes.GetTimerId() {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_CANCEL_TIMER:
		if e.GetEventType() != enumspb.EVENT_TYPE_TIMER_CANCELED {
			return false
		}
		commandAttributes := d.GetCancelTimerCommandAttributes()
		if e.GetEventType() == enumspb.EVENT_TYPE_TIMER_CANCELED {
			eventAttributes := e.GetTimerCanceledEventAttributes()
			if eventAttributes.GetTimerId() != commandAttributes.GetTimerId() {
				return false
			}
		}

		return true

	case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
		if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_RECORD_MARKER:
		if e.GetEventType() != enumspb.EVENT_TYPE_MARKER_RECORDED {
			return false
		}
		eventAttributes := e.GetMarkerRecordedEventAttributes()
		commandAttributes := d.GetRecordMarkerCommandAttributes()
		if eventAttributes.GetMarkerName() != commandAttributes.GetMarkerName() {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		if e.GetEventType() != enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED {
			return false
		}
		eventAttributes := e.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
		commandAttributes := d.GetRequestCancelExternalWorkflowExecutionCommandAttributes()
		if checkNamespacesInCommandAndEvent(eventAttributes.GetNamespace(), commandAttributes.GetNamespace()) || //lint:ignore SA1019 deprecated namespace field
			eventAttributes.WorkflowExecution.GetWorkflowId() != commandAttributes.GetWorkflowId() {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
		if e.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED {
			return false
		}
		eventAttributes := e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()
		commandAttributes := d.GetSignalExternalWorkflowExecutionCommandAttributes()
		if checkNamespacesInCommandAndEvent(eventAttributes.GetNamespace(), commandAttributes.GetNamespace()) || //lint:ignore SA1019 deprecated namespace field
			eventAttributes.GetSignalName() != commandAttributes.GetSignalName() ||
			eventAttributes.WorkflowExecution.GetWorkflowId() != commandAttributes.Execution.GetWorkflowId() {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
		if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
			return false
		}
		return true

	case enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
		if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
		if e.GetEventType() != enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			return false
		}
		eventAttributes := e.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		commandAttributes := d.GetStartChildWorkflowExecutionCommandAttributes()
		if lastPartOfName(eventAttributes.WorkflowType.GetName()) != lastPartOfName(commandAttributes.WorkflowType.GetName()) {
			return false
		}

		return true

	case enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		if e.GetEventType() != enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES {
			return false
		}
		return true

	case enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES:
		if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED {
			return false
		}
		return true

	case enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION:
		if e.GetEventType() != enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED {
			return false
		}
		eventAttributes := e.GetNexusOperationScheduledEventAttributes()
		commandAttributes := d.GetScheduleNexusOperationCommandAttributes()

		return eventAttributes.GetService() == commandAttributes.GetService() &&
			eventAttributes.GetOperation() == commandAttributes.GetOperation()

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION:
		if e.GetEventType() != enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED {
			return false
		}

		eventAttributes := e.GetNexusOperationCancelRequestedEventAttributes()
		commandAttributes := d.GetRequestCancelNexusOperationCommandAttributes()

		return eventAttributes.GetScheduledEventId() == commandAttributes.GetScheduledEventId()
	}

	return false
}

func isSearchAttributesMatched(attrFromEvent, attrFromCommand *commonpb.SearchAttributes) bool {
	if attrFromEvent != nil && attrFromCommand != nil {
		return reflect.DeepEqual(attrFromEvent.IndexedFields, attrFromCommand.IndexedFields)
	}
	return attrFromEvent == nil && attrFromCommand == nil
}

func isMemoMatched(attrFromEvent, attrFromCommand *commonpb.Memo) bool {
	if attrFromEvent != nil && attrFromCommand != nil {
		return reflect.DeepEqual(attrFromEvent.Fields, attrFromCommand.Fields)
	}
	return attrFromEvent == nil && attrFromCommand == nil
}

// return true if the check fails:
//
//	namespace is not empty in command
//	and namespace is not replayNamespace
//	and namespaces unmatch in command and events
func checkNamespacesInCommandAndEvent(eventNamespace, commandNamespace string) bool {
	if commandNamespace == "" || IsReplayNamespace(commandNamespace) {
		return false
	}
	return eventNamespace != commandNamespace
}

func (wth *workflowTaskHandlerImpl) completeWorkflow(
	eventHandler *workflowExecutionEventHandlerImpl,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	workflowContext *workflowExecutionContextImpl,
	commands []*commandpb.Command,
	messages []*protocolpb.Message,
	forceNewWorkflowTask bool,
) workflowTaskCompletion {
	// for query task
	if task.Query != nil {
		queryCompletedRequest := &workflowservice.RespondQueryTaskCompletedRequest{
			TaskToken: task.TaskToken,
			Namespace: wth.namespace,
		}
		var panicErr *PanicError
		if errors.As(workflowContext.err, &panicErr) {
			queryCompletedRequest.CompletedType = enumspb.QUERY_RESULT_TYPE_FAILED
			queryCompletedRequest.ErrorMessage = "Workflow panic: " + panicErr.Error()
			return workflowTaskCompletion{rawRequest: queryCompletedRequest}
		}

		result, err := eventHandler.ProcessQuery(task.Query.GetQueryType(), task.Query.QueryArgs, task.Query.Header)
		if err != nil {
			queryCompletedRequest.CompletedType = enumspb.QUERY_RESULT_TYPE_FAILED
			queryCompletedRequest.ErrorMessage = err.Error()
			wfCtx := converter.WorkflowSerializationContext{
				Namespace:  eventHandler.workflowInfo.Namespace,
				WorkflowID: eventHandler.workflowInfo.WorkflowExecution.ID,
			}
			fc := converter.WithFailureConverterSerializationContext(wth.failureConverter, wfCtx)
			queryCompletedRequest.Failure = fc.ErrorToFailure(err)
		} else {
			queryCompletedRequest.CompletedType = enumspb.QUERY_RESULT_TYPE_ANSWERED
			queryCompletedRequest.QueryResult = result
		}
		return workflowTaskCompletion{rawRequest: queryCompletedRequest}
	}

	// complete workflow task
	var closeCommand *commandpb.Command
	var canceledErr *CanceledError
	var contErr *ContinueAsNewError
	var metricCounterToIncrement string

	if errors.As(workflowContext.err, &canceledErr) {
		// Workflow canceled
		metricCounterToIncrement = metrics.WorkflowCanceledCounter
		closeCommand = createNewCommand(enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION)
		closeCommand.Attributes = &commandpb.Command_CancelWorkflowExecutionCommandAttributes{CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{
			Details: convertErrDetailsToPayloads(canceledErr.details, wth.dataConverter),
		}}
	} else if errors.As(workflowContext.err, &contErr) {
		// Continue as new error.
		metricCounterToIncrement = metrics.WorkflowContinueAsNewCounter
		closeCommand = createNewCommand(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION)

		// ContinueAsNewError.RetryPolicy is optional.
		// If not set, use the retry policy from the workflow context.
		retryPolicy := contErr.RetryPolicy
		if retryPolicy == nil {
			retryPolicy = workflowContext.workflowInfo.RetryPolicy
		}

		useCompat := determineInheritBuildIdFlagForCommand(
			contErr.VersioningIntent, workflowContext.workflowInfo.TaskQueueName, contErr.TaskQueueName)
		closeCommand.Attributes = &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
			WorkflowType:              &commonpb.WorkflowType{Name: contErr.WorkflowType.Name},
			Input:                     contErr.Input,
			TaskQueue:                 &taskqueuepb.TaskQueue{Name: contErr.TaskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowRunTimeout:        durationpb.New(contErr.WorkflowRunTimeout),
			WorkflowTaskTimeout:       durationpb.New(contErr.WorkflowTaskTimeout),
			Header:                    contErr.Header,
			Memo:                      workflowContext.workflowInfo.Memo,
			SearchAttributes:          workflowContext.workflowInfo.SearchAttributes,
			RetryPolicy:               convertToPBRetryPolicy(retryPolicy),
			InheritBuildId:            useCompat,
			InitialVersioningBehavior: continueAsNewVersioningBehaviorToProto(contErr.InitialVersioningBehavior),
		}}
	} else if workflowContext.err != nil {
		// Workflow failures
		if !isBenignApplicationError(workflowContext.err) {
			metricCounterToIncrement = metrics.WorkflowFailedCounter
		}
		closeCommand = createNewCommand(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION)
		wfInfo := eventHandler.workflowInfo
		wfCtx := converter.WorkflowSerializationContext{
			Namespace:  wfInfo.Namespace,
			WorkflowID: wfInfo.WorkflowExecution.ID,
		}
		fc := converter.WithFailureConverterSerializationContext(wth.failureConverter, wfCtx)
		failure := fc.ErrorToFailure(workflowContext.err)
		closeCommand.Attributes = &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
			Failure: failure,
		}}
	} else if workflowContext.isWorkflowCompleted {
		// Workflow completion
		metricCounterToIncrement = metrics.WorkflowCompletedCounter
		closeCommand = createNewCommand(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION)
		closeCommand.Attributes = &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: workflowContext.result,
		}}
	}

	if closeCommand != nil {
		commands = append(commands, closeCommand)
		forceNewWorkflowTask = false
	}

	var queryResults map[string]*querypb.WorkflowQueryResult
	if len(task.Queries) != 0 {
		queryResults = make(map[string]*querypb.WorkflowQueryResult)
		for queryID, query := range task.Queries {
			result, err := eventHandler.ProcessQuery(query.GetQueryType(), query.QueryArgs, query.Header)
			if err != nil {
				queryResults[queryID] = &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
					ErrorMessage: err.Error(),
				}
			} else {
				queryResults[queryID] = &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:     result,
				}
			}
		}
	}

	nonfirstLAAttempts := eventHandler.GatherLAAttemptsThisWFT()

	sdkFlags := eventHandler.sdkFlags.gatherNewSDKFlags()
	langUsedFlags := make([]uint32, 0, len(sdkFlags))
	for _, flag := range sdkFlags {
		langUsedFlags = append(langUsedFlags, uint32(flag))
	}

	seriesName := ""
	if (wth.workerDeploymentVersion != WorkerDeploymentVersion{}) {
		seriesName = wth.workerDeploymentVersion.DeploymentName
	}
	builtRequest := &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken:                  task.TaskToken,
		Commands:                   commands,
		Messages:                   messages,
		Identity:                   wth.identity,
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: forceNewWorkflowTask,
		BinaryChecksum:             wth.workerBuildID,
		QueryResults:               queryResults,
		Namespace:                  wth.namespace,
		MeteringMetadata:           &commonpb.MeteringMetadata{NonfirstLocalActivityExecutionAttempts: nonfirstLAAttempts},
		SdkMetadata: &sdk.WorkflowTaskCompletedMetadata{
			LangUsedFlags: langUsedFlags,
			SdkName:       eventHandler.getNewSdkNameAndReset(),
			SdkVersion:    eventHandler.getNewSdkVersionAndReset(),
		},
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{
			BuildId:       wth.workerBuildID,
			UseVersioning: wth.useBuildIDForVersioning,
		},
		Capabilities: &workflowservice.RespondWorkflowTaskCompletedRequest_Capabilities{
			DiscardSpeculativeWorkflowTaskWithEvents: true,
		},
		Deployment: &deploymentpb.Deployment{
			BuildId:    wth.workerBuildID,
			SeriesName: seriesName,
		},
		DeploymentOptions: workerDeploymentOptionsToProto(
			wth.useBuildIDForVersioning,
			wth.workerDeploymentVersion,
		),
	}
	if wth.capabilities != nil && wth.capabilities.BuildIdBasedVersioning {
		//lint:ignore SA1019 ignore deprecated versioning APIs
		builtRequest.BinaryChecksum = ""
	}
	if wth.useBuildIDForVersioning || (wth.workerDeploymentVersion != WorkerDeploymentVersion{}) {
		workflowType := workflowContext.workflowInfo.WorkflowType
		if behavior, ok := wth.registry.getWorkflowVersioningBehavior(workflowType); ok {
			builtRequest.VersioningBehavior = versioningBehaviorToProto(behavior)
		} else {
			builtRequest.VersioningBehavior = versioningBehaviorToProto(wth.defaultVersioningBehavior)
		}
	}

	// Return request and a function that will update certain metrics
	metricsHandler := wth.metricsHandler.WithTags(metrics.WorkflowTags(
		eventHandler.workflowEnvironmentImpl.workflowInfo.WorkflowType.Name))
	return workflowTaskCompletion{
		rawRequest: builtRequest,
		applyCompletionMetrics: func() {
			if metricCounterToIncrement != "" {
				metricsHandler.Counter(metricCounterToIncrement).Inc(1)
			}
			if closeCommand != nil {
				elapsed := time.Since(workflowContext.workflowInfo.WorkflowStartTime)
				metricsHandler.Timer(metrics.WorkflowEndToEndLatency).Record(elapsed)
			}
		},
	}
}

func (wth *workflowTaskHandlerImpl) executeAnyPressurePoints(event *historypb.HistoryEvent, isInReplay bool) error {
	if wth.ppMgr != nil && !reflect.ValueOf(wth.ppMgr).IsNil() && !isInReplay {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
			return wth.ppMgr.Execute(pressurePointTypeWorkflowTaskStartTimeout)
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			return wth.ppMgr.Execute(pressurePointTypeActivityTaskScheduleTimeout)
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			return wth.ppMgr.Execute(pressurePointTypeActivityTaskStartTimeout)
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			return wth.ppMgr.Execute(pressurePointTypeWorkflowTaskCompleted)
		}
	}
	return nil
}

func newActivityTaskHandler(
	client *WorkflowClient,
	params workerExecutionParameters,
	registry *registry,
) ActivityTaskHandler {
	return newActivityTaskHandlerWithCustomProvider(client, params, registry, nil)
}

func newActivityTaskHandlerWithCustomProvider(
	client *WorkflowClient,
	params workerExecutionParameters,
	registry *registry,
	activityProvider activityProvider,
) ActivityTaskHandler {
	seriesName := ""
	if (params.DeploymentOptions.Version != WorkerDeploymentVersion{}) {
		seriesName = params.DeploymentOptions.Version.DeploymentName
	}
	return &activityTaskHandlerImpl{
		taskQueueName:                    params.TaskQueue,
		identity:                         params.Identity,
		client:                           client,
		logger:                           params.Logger,
		metricsHandler:                   params.MetricsHandler,
		backgroundContext:                params.BackgroundContext,
		registry:                         registry,
		activityProvider:                 activityProvider,
		dataConverter:                    params.DataConverter,
		failureConverter:                 params.FailureConverter,
		workerStopCh:                     params.WorkerStopChannel,
		contextPropagators:               params.ContextPropagators,
		namespace:                        params.Namespace,
		defaultHeartbeatThrottleInterval: params.DefaultHeartbeatThrottleInterval,
		maxHeartbeatThrottleInterval:     params.MaxHeartbeatThrottleInterval,
		versionStamp: &commonpb.WorkerVersionStamp{
			BuildId:       params.getBuildID(),
			UseVersioning: params.UseBuildIDForVersioning,
		},
		deployment: &deploymentpb.Deployment{
			BuildId:    params.getBuildID(),
			SeriesName: seriesName,
		},
		workerDeploymentOptions: workerDeploymentOptionsToProto(
			params.UseBuildIDForVersioning,
			params.DeploymentOptions.Version,
		),
		inboundPayloadVisitor:     params.inboundPayloadVisitor,
		outboundPayloadVisitor:    params.outboundPayloadVisitor,
		payloadVisitorConcurrency: params.payloadVisitorConcurrency,
	}
}

// heartbeatVisitorError wraps an outbound payload visitor error from a heartbeat.
// It is used as the context cancellation cause so Execute() can detect that
// RespondActivityTaskFailed was already sent proactively and skip sending a second response.
type heartbeatVisitorError struct{ err error }

func (e heartbeatVisitorError) Error() string { return e.err.Error() }
func (e heartbeatVisitorError) Unwrap() error { return e.err }

type temporalInvoker struct {
	sync.Mutex
	identity       string
	service        workflowservice.WorkflowServiceClient
	metricsHandler metrics.Handler
	taskToken      []byte
	// cancelHandler is called when the activity is canceled by a heartbeat request.
	cancelHandler context.CancelCauseFunc
	// Amount of time to wait between each pending heartbeat send
	heartbeatThrottleInterval time.Duration
	hbBatchEndTimer           *time.Timer // Whether we started a batch of operations that need to be reported in the cycle. This gets started on a user call.
	lastDetailsToReport       **commonpb.Payloads
	closeCh                   chan struct{}
	workerStopChannel         <-chan struct{}
	namespace                 string
	excludeInternalFromRetry  *atomic.Bool // borrowed from client in order to tell if internal errors are retriable
	outboundPayloadVisitor    PayloadVisitor
	failureConverter          converter.FailureConverter
}

func (i *temporalInvoker) Heartbeat(ctx context.Context, details *commonpb.Payloads, skipBatching bool) error {
	i.Lock()
	defer i.Unlock()

	if i.hbBatchEndTimer != nil && !skipBatching {
		// If we have started batching window, keep track of last reported progress.
		i.lastDetailsToReport = &details
		return nil
	}

	isActivityCanceled, err := i.internalHeartBeat(ctx, details)

	// If the activity is canceled, the activity can ignore the cancellation and do its work
	// and complete. Our cancellation is co-operative, so we will try to heartbeat.
	if (err == nil || isActivityCanceled) && !skipBatching {
		// We have successfully sent heartbeat, start next batching window.
		i.lastDetailsToReport = nil

		// Create timer to fire before the threshold to report.
		i.hbBatchEndTimer = time.NewTimer(i.heartbeatThrottleInterval)

		go func() {
			select {
			case <-i.hbBatchEndTimer.C:
				// We are close to deadline.
			case <-i.workerStopChannel:
				// Activity worker is close to stop. This does the same steps as batch timer ends.
			case <-i.closeCh:
				// We got closed.
				return
			}

			// We close the batch and report the progress.
			var detailsToReport **commonpb.Payloads

			i.Lock()
			detailsToReport = i.lastDetailsToReport
			i.hbBatchEndTimer.Stop()
			i.hbBatchEndTimer = nil
			i.Unlock()

			if detailsToReport != nil {
				// TODO: there is a potential race condition here as the lock is released here and
				// locked again in the Hearbeat() method. This possible that a heartbeat call from
				// user activity grabs the lock first and calls internalHeartBeat before this
				// batching goroutine, which means some activity progress will be lost.
				_ = i.Heartbeat(ctx, *detailsToReport, false)
			}
		}()
	}

	return err
}

func (i *temporalInvoker) internalHeartBeat(ctx context.Context, details *commonpb.Payloads) (bool, error) {
	isActivityCanceled := false
	// We don't want the recording of the heartbeat to keep retrying the RPC
	// longer than the throttle interval. However, sometimes the interval is so
	// small that the context is cancelled before it even starts the call.
	// Therefore, we'll make sure not to timeout the context faster than the
	// minimum RPC timeout.
	recordTimeout := i.heartbeatThrottleInterval
	if recordTimeout < minRPCTimeout {
		recordTimeout = minRPCTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()

	request := &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: i.taskToken,
		Details:   details,
		Identity:  i.identity,
		Namespace: i.namespace,
	}
	var err error
	if visitErr := visitProtoPayloads(ctx, i.outboundPayloadVisitor, request, 0); visitErr != nil {
		// Proactively fail the task so the server can retry immediately rather than
		// waiting for the heartbeat timeout. Errors are ignored — if the RPC fails the
		// activity will still be timed out by the server.
		failReq := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: i.taskToken,
			Failure:   i.failureConverter.ErrorToFailure(visitErr),
			Identity:  i.identity,
			Namespace: i.namespace,
		}
		failCtx, failCancel := context.WithTimeout(context.Background(), recordTimeout)
		defer failCancel()
		_, _ = i.service.RespondActivityTaskFailed(failCtx, failReq)
		err = heartbeatVisitorError{visitErr}
		i.cancelHandler(err)
	} else {
		err = recordActivityHeartbeat(ctx, i.service, i.metricsHandler, request)
	}

	switch err.(type) {
	case *CanceledError:
		// We are asked to cancel. inform the activity about cancellation through context.
		i.cancelHandler(err)
		isActivityCanceled = true
	case *serviceerror.NotFound, *serviceerror.NamespaceNotActive, *serviceerror.NamespaceNotFound:
		// We will pass these through as cancellation for now but something we can change
		// later when we have setter on cancel handler.
		i.cancelHandler(err)
		isActivityCanceled = true
	case nil:
		// No error, do nothing.
	default:
		if errors.Is(err, ErrActivityPaused) || errors.Is(err, ErrActivityReset) {
			// We are asked to pause/reset. inform the activity about cancellation through context.
			i.cancelHandler(err)
			isActivityCanceled = true
		}
		// Transient errors are getting retried for the duration of the heartbeat timeout.
		// The fact that error has been returned means that activity should now be timed out, hence we should
		// propagate cancellation to the handler.
		if retry.IsRetryable(err, i.excludeInternalFromRetry) {
			i.cancelHandler(err)
			isActivityCanceled = true
		}
	}

	if err != nil {
		logger := GetActivityLogger(ctx)
		logger.Warn("RecordActivityHeartbeat with error", tagError, err)
	}

	// This error won't be returned to user check RecordActivityHeartbeat().
	return isActivityCanceled, err
}

func (i *temporalInvoker) Close(ctx context.Context, flushBufferedHeartbeat bool) {
	i.Lock()
	defer i.Unlock()
	close(i.closeCh)
	if i.hbBatchEndTimer != nil {
		i.hbBatchEndTimer.Stop()
		if flushBufferedHeartbeat && i.lastDetailsToReport != nil {
			_, _ = i.internalHeartBeat(ctx, *i.lastDetailsToReport)
			i.lastDetailsToReport = nil
		}
	}
}

func (i *temporalInvoker) GetClient(options ClientOptions) Client {
	return NewServiceClient(i.service, nil, options)
}

func newServiceInvoker(
	taskToken []byte,
	identity string,
	service workflowservice.WorkflowServiceClient,
	metricsHandler metrics.Handler,
	cancelHandler context.CancelCauseFunc,
	heartbeatThrottleInterval time.Duration,
	workerStopChannel <-chan struct{},
	namespace string,
	excludeInternalFromRetry *atomic.Bool,
	outboundPayloadVisitor PayloadVisitor,
	failureConverter converter.FailureConverter,
) ServiceInvoker {
	return &temporalInvoker{
		taskToken:                 taskToken,
		identity:                  identity,
		service:                   service,
		metricsHandler:            metricsHandler,
		cancelHandler:             cancelHandler,
		heartbeatThrottleInterval: heartbeatThrottleInterval,
		closeCh:                   make(chan struct{}),
		workerStopChannel:         workerStopChannel,
		namespace:                 namespace,
		excludeInternalFromRetry:  excludeInternalFromRetry,
		outboundPayloadVisitor:    outboundPayloadVisitor,
		failureConverter:          failureConverter,
	}
}

// Execute executes an implementation of the activity.
func (ath *activityTaskHandlerImpl) Execute(taskQueue string, t *workflowservice.PollActivityTaskQueueResponse) (result interface{}, err error) {
	traceLog(func() {
		if t.WorkflowExecution.GetWorkflowId() == "" {
			ath.logger.Debug("Processing new standalone activity task",
				tagActivityID, t.ActivityId,
				tagActivityRunID, t.ActivityRunId,
				tagActivityType, t.ActivityType.GetName(),
				tagAttempt, t.Attempt,
			)
		} else {
			ath.logger.Debug("Processing new workflow activity task",
				tagWorkflowID, t.WorkflowExecution.GetWorkflowId(),
				tagRunID, t.WorkflowExecution.GetRunId(),
				tagActivityID, t.ActivityId,
				tagActivityType, t.ActivityType.GetName(),
				tagAttempt, t.Attempt,
			)
		}
	})
	actCtx := converter.ActivitySerializationContext{
		Namespace:    ath.namespace,
		WorkflowID:   t.WorkflowExecution.GetWorkflowId(),
		WorkflowType: t.WorkflowType.GetName(),
		ActivityType: t.ActivityType.GetName(),
		TaskQueue:    taskQueue,
	}
	dataConverter := converter.WithDataConverterSerializationContext(ath.dataConverter, actCtx)
	failureConverter := converter.WithFailureConverterSerializationContext(ath.failureConverter, actCtx)

	// The root context is only cancelled when the worker is finished shutting down.
	rootCtx := ath.backgroundContext
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	canCtx, cancel := context.WithCancelCause(rootCtx)
	defer cancel(nil)

	if err := visitProtoPayloads(canCtx, ath.inboundPayloadVisitor, t, ath.payloadVisitorConcurrency); err != nil {
		return ath.visitorErrorToActivityFailure("Activity task preprocess error: ", t, err), nil
	}

	heartbeatThrottleInterval := ath.getHeartbeatThrottleInterval(t.GetHeartbeatTimeout().AsDuration())
	invoker := newServiceInvoker(
		t.TaskToken, ath.identity, ath.client.workflowService, ath.metricsHandler, cancel, heartbeatThrottleInterval,
		ath.workerStopCh, ath.namespace, ath.client.excludeInternalFromRetry, ath.outboundPayloadVisitor, failureConverter)

	workflowType := t.WorkflowType.GetName()
	activityType := t.ActivityType.GetName()
	metricsHandler := ath.metricsHandler.WithTags(metrics.ActivityTags(workflowType, activityType, ath.taskQueueName))
	ctx, err := WithActivityTask(canCtx, t, taskQueue, invoker, ath.logger, metricsHandler,
		ath.dataConverter, ath.workerStopCh, ath.contextPropagators, ath.registry.interceptors, ath.client)
	if err != nil {
		return nil, err
	}

	// We must capture the context here because it is changed later to one that is
	// cancelled when the activity is done
	defer func(ctx context.Context) {
		_, activityCompleted := result.(*workflowservice.RespondActivityTaskCompletedRequest)
		invoker.Close(ctx, !activityCompleted) // flush buffered heartbeat if activity was not successfully completed.
	}(ctx)

	activityImplementation := ath.getActivity(activityType)
	if activityImplementation == nil {
		// In case if activity is not registered we should report a failure to the server to allow activity retry
		// instead of making it stuck on the same attempt.
		metricsHandler.Counter(metrics.UnregisteredActivityInvocationCounter).Inc(1)
		return convertActivityResultToRespondRequest(ath.identity, t.TaskToken, nil,
			NewActivityNotRegisteredError(activityType, ath.getRegisteredActivityNames()),
			dataConverter, failureConverter, ath.namespace, false, ath.versionStamp, ath.deployment, ath.workerDeploymentOptions), nil
	}

	// panic handler
	defer func() {
		if p := recover(); p != nil {
			topLine := fmt.Sprintf("activity for %s [panic]:", ath.taskQueueName)
			st := getStackTraceRaw(topLine, 7, 0)
			ath.logger.Error("Activity panic.",
				tagWorkflowID, t.WorkflowExecution.GetWorkflowId(),
				tagRunID, t.WorkflowExecution.GetRunId(),
				tagActivityType, activityType,
				tagAttempt, t.Attempt,
				tagPanicError, fmt.Sprintf("%v", p),
				tagPanicStack, st)
			metricsHandler.Counter(metrics.ActivityTaskErrorCounter).Inc(1)
			panicErr := newPanicError(p, st)
			result = convertActivityResultToRespondRequest(ath.identity, t.TaskToken, nil, panicErr,
				dataConverter, failureConverter, ath.namespace, false, ath.versionStamp, ath.deployment, ath.workerDeploymentOptions)
		}
	}()

	// propagate context information into the activity context from the headers
	ctx, err = contextWithHeaderPropagated(ctx, t.Header, ath.contextPropagators)
	if err != nil {
		return nil, err
	}

	info := getActivityEnv(ctx)
	ctx, dlCancelFunc := context.WithDeadline(ctx, info.deadline)
	defer dlCancelFunc()

	output, err := activityImplementation.Execute(ctx, t.Input)
	// Check if context canceled at a higher level before we cancel it ourselves

	// The heartbeat visitor failure path proactively sent RespondActivityTaskFailed,
	// skip sending another response regardless of what the activity returned.
	var hbVisitorErr heartbeatVisitorError
	if errors.As(context.Cause(canCtx), &hbVisitorErr) {
		return nil, nil
	}

	// Cancels that don't originate from the server will have separate cancel reasons, like
	// ErrWorkerShutdown or ErrActivityPaused
	isActivityCanceled := ctx.Err() == context.Canceled && IsCanceledError(context.Cause(ctx))

	dlCancelFunc()
	if <-ctx.Done(); ctx.Err() == context.DeadlineExceeded {
		ath.logger.Info("Activity complete after timeout.",
			tagWorkflowID, t.WorkflowExecution.GetWorkflowId(),
			tagRunID, t.WorkflowExecution.GetRunId(),
			tagActivityType, activityType,
			tagAttempt, t.Attempt,
			tagResult, output,
			tagError, err,
		)
		return nil, ctx.Err()
	}
	if err != nil && err != ErrActivityResultPending {
		logFunc := ath.logger.Error // Default to Error
		if isBenignApplicationError(err) {
			logFunc = ath.logger.Debug // Downgrade to Debug for benign application errors
		}
		logFunc("Activity error.",
			tagWorkflowID, t.WorkflowExecution.GetWorkflowId(),
			tagRunID, t.WorkflowExecution.GetRunId(),
			tagActivityType, activityType,
			tagAttempt, t.Attempt,
			tagError, err,
		)
	}

	response := convertActivityResultToRespondRequest(ath.identity, t.TaskToken, output, err,
		dataConverter, failureConverter, ath.namespace, isActivityCanceled, ath.versionStamp, ath.deployment, ath.workerDeploymentOptions)

	if msg, ok := response.(proto.Message); ok {
		var storageTarget converter.StorageDriverTargetInfo
		if t.WorkflowExecution.GetWorkflowId() != "" {
			storageTarget = converter.StorageDriverWorkflowInfo{
				Namespace:    ath.namespace,
				WorkflowID:   t.WorkflowExecution.GetWorkflowId(),
				RunID:        t.WorkflowExecution.GetRunId(),
				WorkflowType: t.WorkflowType.GetName(),
			}
		} else {
			storageTarget = converter.StorageDriverActivityInfo{
				Namespace:    ath.namespace,
				ActivityID:   t.ActivityId,
				RunID:        t.ActivityRunId,
				ActivityType: t.ActivityType.GetName(),
			}
		}
		// Use backgroundContext as base so a cancelled activity context (e.g. pause/reset)
		// does not prevent the outbound storage visitor from making HTTP calls.
		outboundBase := ath.backgroundContext
		if outboundBase == nil {
			outboundBase = context.Background()
		}
		outboundCtx := extstore.WithStorageTarget(outboundBase, storageTarget)
		if err := visitProtoPayloads(outboundCtx, ath.outboundPayloadVisitor, msg, ath.payloadVisitorConcurrency); err != nil {
			return ath.visitorErrorToActivityFailure("Activity task postprocess error: ", t, err), nil
		}
	}

	return response, nil
}

func (ath *activityTaskHandlerImpl) visitorErrorToActivityFailure(msgPrefix string, t *workflowservice.PollActivityTaskQueueResponse, err error) *workflowservice.RespondActivityTaskFailedRequest {
	keyvals := []any{
		tagWorkflowID, t.WorkflowExecution.GetWorkflowId(),
		tagRunID, t.WorkflowExecution.GetRunId(),
		tagActivityType, t.ActivityType.Name,
		tagAttempt, t.Attempt,
	}

	var errPayloadSize payloadSizeError
	if errors.As(err, &errPayloadSize) {
		keyvals = append(keyvals,
			tagPayloadSize, errPayloadSize.size,
			tagPayloadSizeLimit, errPayloadSize.limit)
	}

	ath.logger.Error(msgPrefix+err.Error(), keyvals...)

	return &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken:         t.TaskToken,
		Failure:           ath.failureConverter.ErrorToFailure(err),
		Identity:          ath.identity,
		Namespace:         ath.namespace,
		WorkerVersion:     ath.versionStamp,
		Deployment:        ath.deployment,
		DeploymentOptions: ath.workerDeploymentOptions,
	}
}

func (ath *activityTaskHandlerImpl) getActivity(name string) activity {
	if ath.activityProvider != nil {
		return ath.activityProvider(name)
	}

	if a, ok := ath.registry.GetActivity(name); ok {
		return a
	}

	return nil
}

func (ath *activityTaskHandlerImpl) getRegisteredActivityNames() (activityNames []string) {
	for _, a := range ath.registry.getRegisteredActivities() {
		activityNames = append(activityNames, a.ActivityType().Name)
	}
	return
}

func (ath *activityTaskHandlerImpl) getHeartbeatThrottleInterval(heartbeatTimeout time.Duration) time.Duration {
	// Set interval as 80% of timeout if present, or the configured default if
	// present, or the system default otherwise
	var heartbeatThrottleInterval time.Duration
	if heartbeatTimeout > 0 {
		heartbeatThrottleInterval = time.Duration(0.8 * float64(heartbeatTimeout))
	} else if ath.defaultHeartbeatThrottleInterval > 0 {
		heartbeatThrottleInterval = ath.defaultHeartbeatThrottleInterval
	} else {
		heartbeatThrottleInterval = defaultDefaultHeartbeatThrottleInterval
	}

	// Use the configured max if present, or the system default otherwise
	maxHeartbeatThrottleInterval := ath.maxHeartbeatThrottleInterval
	if maxHeartbeatThrottleInterval == 0 {
		maxHeartbeatThrottleInterval = defaultMaxHeartbeatThrottleInterval
	}

	// Limit interval to a max
	if heartbeatThrottleInterval > maxHeartbeatThrottleInterval {
		heartbeatThrottleInterval = maxHeartbeatThrottleInterval
	}
	return heartbeatThrottleInterval
}

func createNewCommand(commandType enumspb.CommandType) *commandpb.Command {
	return &commandpb.Command{
		CommandType: commandType,
	}
}

func createNewCommandWithMetadata(commandType enumspb.CommandType, metadata *sdk.UserMetadata) *commandpb.Command {
	return &commandpb.Command{
		CommandType:  commandType,
		UserMetadata: metadata,
	}
}

func recordActivityHeartbeat(ctx context.Context, service workflowservice.WorkflowServiceClient, metricsHandler metrics.Handler,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) error {
	var heartbeatResponse *workflowservice.RecordActivityTaskHeartbeatResponse
	grpcCtx, cancel := newGRPCContext(ctx,
		grpcMetricsHandler(metricsHandler),
		defaultGrpcRetryParameters(ctx))
	defer cancel()

	heartbeatResponse, err := service.RecordActivityTaskHeartbeat(grpcCtx, request)
	if err == nil && heartbeatResponse != nil {
		if heartbeatResponse.GetCancelRequested() {
			return NewCanceledError()
		} else if heartbeatResponse.GetActivityPaused() {
			return ErrActivityPaused
		} else if heartbeatResponse.GetActivityReset() {
			return ErrActivityReset
		}
	}
	return err
}

func recordActivityHeartbeatByID(ctx context.Context, service workflowservice.WorkflowServiceClient, metricsHandler metrics.Handler,
	request *workflowservice.RecordActivityTaskHeartbeatByIdRequest,
) error {
	var heartbeatResponse *workflowservice.RecordActivityTaskHeartbeatByIdResponse
	grpcCtx, cancel := newGRPCContext(ctx,
		grpcMetricsHandler(metricsHandler),
		defaultGrpcRetryParameters(ctx))
	defer cancel()

	heartbeatResponse, err := service.RecordActivityTaskHeartbeatById(grpcCtx, request)
	if err == nil && heartbeatResponse != nil && heartbeatResponse.GetCancelRequested() {
		return NewCanceledError()
	}
	return err
}

// This enables verbose logging in the client library.
// check worker.EnableVerboseLogging()
func traceLog(fn func()) {
	if enableVerboseLogging {
		fn()
	}
}
