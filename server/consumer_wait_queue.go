package server

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// ----------------------------------------------------------
// Shared Data Structures
// ----------------------------------------------------------

// WaitingRequest represents a request in the queue
type WaitingRequest struct {
	next          *WaitingRequest
	acc           *Account
	interest      string
	reply         string
	n             int // For batching
	d             int // num delivered
	b             int // For max bytes tracking
	expires       time.Time
	received      time.Time
	hb            time.Duration
	hbt           time.Time
	noWait        bool
	priorityGroup *PriorityGroup
}

// recycleIfDone recycles this request if n <= 0
func (wr *WaitingRequest) recycleIfDone() bool {
	if wr != nil && wr.n <= 0 {
		wr.recycle()
		return true
	}
	return false
}

// recycle forces a recycle of this request
func (wr *WaitingRequest) recycle() {
	if wr != nil {
		wr.next, wr.acc, wr.interest, wr.reply = nil, nil, _EMPTY_, _EMPTY_
		wrPool.Put(wr)
	}
}

// instanceID extracts a worker/instance ID from wr.reply
func (wr *WaitingRequest) instanceID() string {
	if wr != nil && wr.reply != "" {
		parts := strings.Split(wr.reply, ".")
		if len(parts) > 1 {
			return parts[1]
		}
	}
	return ""
}

// ----------------------------------------------------------
// WaitQueue Interface
// ----------------------------------------------------------

type WaitQueue interface {
	// Add adds a new request to the queue
	Add(wr *WaitingRequest) error

	// Peek returns the next request that would be popped without removing it
	Peek() *WaitingRequest

	// Tail returns the last request in the queue
	Tail() *WaitingRequest

	// Pop returns and removes the next request from the queue
	Pop() *WaitingRequest

	// Cycle moves the current head (or flow) to the end if valid
	Cycle()

	// IsFull returns true if the queue is at capacity
	IsFull() bool

	// IsEmpty returns true if the queue has no items
	IsEmpty() bool

	// Len returns the current number of items in the queue
	Len() int

	// RemoveCurrent removes the current head request
	RemoveCurrent()

	// Remove removes a specific request from the queue
	Remove(pre, wr *WaitingRequest)

	// Last returns the last active time
	Last() time.Time

	// SetLast sets the last active time
	SetLast(t time.Time)

	// LogFlows prints information about each active flow in this DRRWaitQueue.
	LogFlows()
}

// ----------------------------------------------------------
// BaseWaitQueue with shared fields
// ----------------------------------------------------------

type BaseWaitQueue struct {
	mu   sync.RWMutex
	n    int
	max  int
	last time.Time
	head *WaitingRequest
	tail *WaitingRequest
}

// Common errors
var (
	ErrWaitQueueFull = errors.New("wait queue is full")
	ErrWaitQueueNil  = errors.New("wait queue is nil")
)

// ----------------------------------------------------------
// Redis, Stake Cache, and Global Map
// ----------------------------------------------------------

var (
	// globalRedis      *redis.Client
	globalStakeCache *stakeCache
	stakeCacheOnce   sync.Once
	waitQueueMap     = make(map[string]*WaitQueueInfo)
	waitQueueMutex   sync.RWMutex
)

// SetGlobalRedis sets up the global Redis client for all wait queues
// func SetGlobalRedis(redisClient *redis.Client) {
// 	globalRedis = redisClient
// }

// stakeCache maintains the current account balances and related stats
type stakeCache struct {
	sync.RWMutex
	stakeByInstanceID map[string]float64
	totalStake        float64
	lastUpdate        time.Time
}

// getStakeCache returns the singleton stake cache instance
func getStakeCache() *stakeCache {
	stakeCacheOnce.Do(func() {
		// if globalRedis == nil {
		//     panic("global Redis client not initialized")
		// }
		globalStakeCache = &stakeCache{
			stakeByInstanceID: map[string]float64{
				"test-worker-id":   1.0,
				"test-worker-2-id": 2.0,
			},
			totalStake: 3.0,
			lastUpdate: time.Now(),
		}
	})
	return globalStakeCache
}

// ----------------------------------------------------------
// FIFOWaitQueue
// ----------------------------------------------------------

type FIFOWaitQueue struct {
	BaseWaitQueue
}

// NewFIFOWaitQueue creates a new FIFO-based wait queue
func NewFIFOWaitQueue(max int) *FIFOWaitQueue {
	return &FIFOWaitQueue{
		BaseWaitQueue: BaseWaitQueue{
			max: max,
		},
	}
}

// Add implements WaitQueue.Add
func (wq *FIFOWaitQueue) Add(wr *WaitingRequest) error {
	if wq == nil {
		return ErrWaitQueueNil
	}
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.n >= wq.max {
		return ErrWaitQueueFull
	}

	if wq.head == nil {
		wq.head = wr
	} else {
		wq.tail.next = wr
	}
	wq.tail = wr
	wr.next = nil
	wq.last = wr.received
	wq.n++
	return nil
}

// Pop implements WaitQueue.Pop
func (wq *FIFOWaitQueue) Pop() *WaitingRequest {
	if wq == nil {
		return nil
	}
	wq.mu.Lock()
	defer wq.mu.Unlock()

	wr := wq.head
	if wr != nil {
		wr.d++
		wr.n--
		// Remove from front
		wq.head = wr.next
		if wq.head == nil {
			wq.tail = nil
		}
		wq.n--

		// If it still has n>0, requeue at the tail
		if wr.n > 0 {
			wr.next = nil
			if wq.head == nil {
				wq.head = wr
				wq.tail = wr
			} else {
				wq.tail.next = wr
				wq.tail = wr
			}
			wq.n++
		} else {
			wr.next = nil
		}
	}
	return wr
}

// IsFull implements WaitQueue.IsFull
func (wq *FIFOWaitQueue) IsFull() bool {
	if wq == nil {
		return false
	}
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.n == wq.max
}

// IsEmpty implements WaitQueue.IsEmpty
func (wq *FIFOWaitQueue) IsEmpty() bool {
	if wq == nil {
		return true
	}
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.n == 0
}

// Len implements WaitQueue.Len
func (wq *FIFOWaitQueue) Len() int {
	if wq == nil {
		return 0
	}
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.n
}

// Peek implements WaitQueue.Peek
func (wq *FIFOWaitQueue) Peek() *WaitingRequest {
	if wq == nil {
		return nil
	}
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.head
}

// Tail implements WaitQueue.Tail
func (wq *FIFOWaitQueue) Tail() *WaitingRequest {
	if wq == nil {
		return nil
	}
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.tail
}

// Cycle implements WaitQueue.Cycle
func (wq *FIFOWaitQueue) Cycle() {
	if wq == nil {
		return
	}
	wq.mu.Lock()
	defer wq.mu.Unlock()

	wr := wq.head
	if wr != nil {
		// Remove from front
		wq.head = wr.next
		if wq.head == nil {
			wq.tail = nil
		} else {
			if wq.n > 0 {
				wq.n--
			}
		}
		// Re-add
		wq.Add(wr)
	}
}

// RemoveCurrent implements WaitQueue.RemoveCurrent
func (wq *FIFOWaitQueue) RemoveCurrent() {
	wq.Remove(nil, wq.Peek())
}

// Remove implements WaitQueue.Remove
func (wq *FIFOWaitQueue) Remove(pre, wr *WaitingRequest) {
	if wq == nil {
		return
	}
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wr == nil {
		return
	}
	if pre != nil {
		pre.next = wr.next
	} else if wr == wq.head {
		wq.head = wr.next
	}
	if wr == wq.tail {
		if wr.next == nil {
			wq.tail = pre
		} else {
			wq.tail = wr.next
		}
	}
	if wq.n > 0 {
		wq.n--
	}
	wr.next = nil
}

// Last implements WaitQueue.Last
func (wq *FIFOWaitQueue) Last() time.Time {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.last
}

// SetLast implements WaitQueue.SetLast
func (wq *FIFOWaitQueue) SetLast(t time.Time) {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	wq.last = t
}

// LogFlows implements WaitQueue.LogFlows
func (wq *FIFOWaitQueue) LogFlows() {
	fmt.Println("FIFOWaitQueue has no flows")
}

// ----------------------------------------------------------
// DRRWaitQueue (Deficit Round Robin) with Single-Item Pop
// ----------------------------------------------------------

// costOfRequest returns the "cost" for a given request (assume 1).
func costOfRequest(_ *WaitingRequest) int {
	return 1
}

// flow represents per-worker DRR state
type flow struct {
	instanceID string
	head       *WaitingRequest
	tail       *WaitingRequest
	next       *flow // for circular linked-list
	deficit    int   // DRR deficit counter
	quantum    int   // DRR quantum (stake-based)
}

// DRRWaitQueue implements WaitQueue using Deficit Round Robin scheduling
type DRRWaitQueue struct {
	BaseWaitQueue

	// Protect DRR-specific fields
	mu sync.Mutex

	flows           map[string]*flow
	activeFlows     *flow
	activeFlowsTail *flow
	current         *flow

	scache      *stakeCache
	baseQuantum int
}

// Ensure DRRWaitQueue implements WaitQueue
var _ WaitQueue = (*DRRWaitQueue)(nil)

// NewDRRWaitQueue creates a new DRRWaitQueue with the given maximum capacity.
func NewDRRWaitQueue(max int) *DRRWaitQueue {
	return &DRRWaitQueue{
		BaseWaitQueue: BaseWaitQueue{
			max: max,
		},
		flows:       make(map[string]*flow),
		scache:      getStakeCache(),
		baseQuantum: 1, // quantum = ceil(stake) * baseQuantum
	}
}

// quantumForStake returns int(math.Ceil(stake)).
func (wq *DRRWaitQueue) quantumForStake(stake float64) int {
	return int(math.Ceil(stake))
}

// activateFlow inserts the flow f into the circular active list
func (wq *DRRWaitQueue) activateFlow(f *flow) {
	if f == nil {
		return
	}
	if wq.activeFlows == nil {
		// first active flow
		wq.activeFlows = f
		wq.activeFlowsTail = f
		f.next = f // circular
		if wq.current == nil {
			wq.current = f
		}
		return
	}
	// If flow might already be in the list (f.next != nil), skip
	if f.next != nil {
		return
	}
	// Insert at tail
	f.next = wq.activeFlows
	wq.activeFlowsTail.next = f
	wq.activeFlowsTail = f
}

// removeFlowFromActive removes flow f from circular list
func (wq *DRRWaitQueue) removeFlowFromActive(f *flow) {
	if f == nil || wq.activeFlows == nil {
		return
	}
	// if single flow in list
	if wq.activeFlows == f && wq.activeFlowsTail == f && f.next == f {
		wq.activeFlows = nil
		wq.activeFlowsTail = nil
		if wq.current == f {
			wq.current = nil
		}
		f.next = nil
		return
	}
	// find predecessor
	prev := f
	for prev.next != f {
		prev = prev.next
	}
	prev.next = f.next
	if wq.activeFlows == f {
		wq.activeFlows = f.next
	}
	if wq.activeFlowsTail == f {
		wq.activeFlowsTail = prev
	}
	if wq.current == f {
		wq.current = f.next
	}
	f.next = nil
}

// Add enqueues wr into the DRR queue
func (wq *DRRWaitQueue) Add(wr *WaitingRequest) error {
	if wr == nil {
		return ErrWaitQueueNil
	}
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.n >= wq.max {
		return ErrWaitQueueFull
	}

	instanceID := wr.instanceID()
	if instanceID == "" {
		instanceID = "unknown"
	}
	stake := wq.scache.stakeByInstanceID[instanceID]
	if stake < 0 {
		stake = 0
	}

	f := wq.flows[instanceID]
	if f == nil {
		// create new flow
		f = &flow{
			instanceID: instanceID,
			deficit:    0,
			quantum:    wq.quantumForStake(stake),
		}
		wq.flows[instanceID] = f
	}

	// enqueue into flow
	if f.head == nil {
		f.head = wr
		f.tail = wr
		// newly active
		wq.activateFlow(f)
	} else {
		f.tail.next = wr
		f.tail = wr
	}

	wq.n++
	wq.last = wr.received
	return nil
}

// Peek returns the next request that would be popped (best effort)
func (wq *DRRWaitQueue) Peek() *WaitingRequest {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.activeFlows == nil || wq.current == nil {
		return nil
	}
	return wq.current.head
}

// Tail returns the last request in the queue
// We return BaseWaitQueue.tail for completeness
func (wq *DRRWaitQueue) Tail() *WaitingRequest {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return wq.BaseWaitQueue.tail
}

// Pop returns one request from DRR (partial dispatch)
func (wq *DRRWaitQueue) Pop() *WaitingRequest {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.activeFlows == nil {
		return nil
	}
	if wq.current == nil {
		wq.current = wq.activeFlows
	}

	count := wq.countActiveFlows()
	if count == 0 {
		return nil
	}

	original := wq.current
	for i := 0; i < count; i++ {
		f := wq.current

		// Add quantum to deficit
		f.deficit += f.quantum

		// If we can afford the head
		if f.head != nil && costOfRequest(f.head) <= f.deficit {
			wr := f.head
			f.head = wr.next
			if f.head == nil {
				f.tail = nil
			}
			f.deficit -= costOfRequest(wr)
			wq.n--
			wr.next = nil

			// if flow is empty now, remove it
			if f.head == nil {
				wq.removeFlowFromActive(f)
			}

			// Rotate to the NEXT flow so we don't keep returning the same flow
			wq.current = f.next
			// Return exactly one item
			return wr
		}

		// rotate to next flow
		wq.current = f.next
		if wq.current == original {
			break
		}
	}
	// none found
	return nil
}

// Cycle moves the current pointer to the next flow
func (wq *DRRWaitQueue) Cycle() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.activeFlows == nil || wq.current == nil {
		return
	}
	wq.current = wq.current.next
}

// RemoveCurrent removes the request at current flow's head
func (wq *DRRWaitQueue) RemoveCurrent() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.current == nil || wq.current.head == nil {
		return
	}
	wr := wq.current.head
	wq.removeRequest(wq.current, nil, wr)
}

// Remove removes a specific request from the queue
func (wq *DRRWaitQueue) Remove(pre, wr *WaitingRequest) {
	if wr == nil {
		return
	}
	wq.mu.Lock()
	defer wq.mu.Unlock()

	instanceID := wr.instanceID()
	if instanceID == "" {
		instanceID = "unknown"
	}
	f := wq.flows[instanceID]
	if f == nil {
		return
	}
	wq.removeRequest(f, pre, wr)
}

// removeRequest unlinks wr from flow f
func (wq *DRRWaitQueue) removeRequest(f *flow, pre, wr *WaitingRequest) {
	if f == nil || wr == nil {
		return
	}
	if f.head == nil {
		return
	}

	if pre == nil {
		// remove head
		if wr == f.head {
			f.head = f.head.next
			if f.head == nil {
				f.tail = nil
			}
			wq.n--
			wr.next = nil
		}
	} else {
		// remove mid or tail
		if pre.next == wr {
			pre.next = wr.next
			if wr == f.tail {
				f.tail = pre
			}
			wq.n--
			wr.next = nil
		}
	}

	// if flow empty, remove it
	if f.head == nil {
		wq.removeFlowFromActive(f)
	}
}

// IsFull returns true if DRR queue is at capacity
func (wq *DRRWaitQueue) IsFull() bool {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return wq.n >= wq.max
}

// IsEmpty returns true if DRR queue has no items
func (wq *DRRWaitQueue) IsEmpty() bool {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return wq.n == 0
}

// Len returns the current number of items in DRR
func (wq *DRRWaitQueue) Len() int {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return wq.n
}

// Last returns the last active time
func (wq *DRRWaitQueue) Last() time.Time {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return wq.last
}

// SetLast sets the last active time
func (wq *DRRWaitQueue) SetLast(t time.Time) {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	wq.last = t
}

// countActiveFlows returns how many flows are in the active list (circular)
func (wq *DRRWaitQueue) countActiveFlows() int {
	if wq.activeFlows == nil {
		return 0
	}
	count := 0
	start := wq.activeFlows
	f := start
	for {
		count++
		f = f.next
		if f == start {
			break
		}
	}
	return count
}

// LogFlows prints information about each active flow in this DRRWaitQueue.
func (wq *DRRWaitQueue) LogFlows() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	// Optionally, log overall queue info:
	fmt.Printf("==== DRRWaitQueue Debug ====\n")
	fmt.Printf("Total Requests: %d / Capacity: %d\n", wq.n, wq.max)
	if wq.activeFlows == nil {
		fmt.Println("No active flows. (Either empty or flows have no requests.)")
		return
	}

	// Walk the circular list of active flows exactly once
	start := wq.activeFlows
	f := start
	for {
		// Count how many requests are currently in this flow
		reqCount := 0
		for wr := f.head; wr != nil; wr = wr.next {
			reqCount++
		}

		// Print relevant details
		fmt.Printf("Flow instance=%q, Deficit=%d, Quantum=%d, Requests=%d\n",
			f.instanceID, f.deficit, f.quantum, reqCount)

		// Move on to the next flow
		f = f.next
		if f == start {
			break // We’ve looped around
		}
	}
	fmt.Println("================================")
}

// ----------------------------------------------------------
// WaitQueueInfo & monitorWaitQueue
// ----------------------------------------------------------

type WaitQueueInfo struct {
	wq     WaitQueue
	stream string
	what   string
}

// NewWaitQueue picks DRR for inference streams, FIFO otherwise
func NewWaitQueue(max int, stream string) WaitQueue {
	fmt.Println("NewWaitQueue:", stream)

	waitQueueMutex.Lock()
	defer waitQueueMutex.Unlock()

	// Check if queue already exists for this stream
	if info, exists := waitQueueMap[stream]; exists {
		return info.wq
	}

	isInference := strings.Contains(stream, "fast") || strings.Contains(stream, "slow")

	var what string
	var wq WaitQueue
	if isInference {
		what = "DRR"
		wq = NewDRRWaitQueue(max)
	} else {
		what = "FIFO"
		wq = NewFIFOWaitQueue(max)
	}

	info := &WaitQueueInfo{
		wq:     wq,
		stream: stream,
		what:   what,
	}
	waitQueueMap[stream] = info
	monitorWaitQueue()

	return wq
}

var isRunning = false

func monitorWaitQueue() {
	if isRunning {
		return
	}
	isRunning = true

	go func() {
		for range time.Tick(1 * time.Second) {
			fmt.Println("--------------------------------")
			// gather streams, sorted
			waitQueueMutex.RLock()
			streams := make([]string, 0, len(waitQueueMap))
			for s := range waitQueueMap {
				streams = append(streams, s)
			}
			waitQueueMutex.RUnlock()
			sort.Strings(streams)

			// print in sorted order
			waitQueueMutex.RLock()
			for _, s := range streams {
				info := waitQueueMap[s]
				// fmt.Printf("| %-4s | %-4d | %-40s |\n", info.what, info.wq.Len(), info.stream)
				info.wq.LogFlows()
			}
			waitQueueMutex.RUnlock()
			fmt.Println("--------------------------------")
		}
	}()
}
