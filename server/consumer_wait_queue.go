package server

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// WaitQueue represents a priority queue that handles waiting requests
type WaitQueue interface {
	// Add adds a new request to the queue
	// Returns errWaitQueueFull if the queue is at capacity
	Add(wr *WaitingRequest) error

	// Peek returns the next request that would be popped without removing it
	// Returns nil if the queue is empty
	Peek() *WaitingRequest

	// Tail returns the last request in the queue
	Tail() *WaitingRequest

	// Pop returns and removes the next request from the queue based on scheduling policy
	// Returns nil if the queue is empty
	Pop() *WaitingRequest

	// Cycle moves the current head request to the end if valid
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
}

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

// Recycle this request. This request can not be accessed after this call.
func (wr *WaitingRequest) recycleIfDone() bool {
	if wr != nil && wr.n <= 0 {
		wr.recycle()
		return true
	}
	return false
}

// Force a recycle.
func (wr *WaitingRequest) recycle() {
	if wr != nil {
		wr.next, wr.acc, wr.interest, wr.reply = nil, nil, _EMPTY_, _EMPTY_
		wrPool.Put(wr)
	}
}

// Common errors
var (
	ErrWaitQueueFull = errors.New("wait queue is full")
	ErrWaitQueueNil  = errors.New("wait queue is nil")
)

// accountState tracks DRR scheduling state for an account
type accountState struct {
	weight  int64 // Weight based on account balance
	deficit int64 // Current deficit in DRR scheduling
}

var (
	globalRedis        *redis.Client
	globalBalanceCache *balanceCache
	balanceCacheOnce   sync.Once
	waitQueueMap       = make(map[string]*WaitQueueInfo)
	waitQueueMutex     sync.RWMutex
)

// SetGlobalRedis sets up the global Redis client for all wait queues
func SetGlobalRedis(redisClient *redis.Client) {
	globalRedis = redisClient
}

// balanceCache maintains the current account balances and related stats
type balanceCache struct {
	sync.RWMutex
	balances     map[string]float64
	totalBalance float64
	lastUpdate   time.Time
}

// getBalanceCache returns the singleton balance cache instance
func getBalanceCache() *balanceCache {
	balanceCacheOnce.Do(func() {
		// if globalRedis == nil {
		// 	panic("global Redis client not initialized")
		// }
		globalBalanceCache = &balanceCache{
			balances: make(map[string]float64),
		}
		// Start balance updater
		go globalBalanceCache.balanceUpdater()
	})
	return globalBalanceCache
}

// balanceUpdater periodically updates balances from Redis
func (bc *balanceCache) balanceUpdater() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := bc.updateBalances(); err != nil {
			// Handle error, maybe log it
			continue
		}
	}
}

// updateBalances fetches current balances and updates weights
func (bc *balanceCache) updateBalances() error {
	// Fetch balances from Redis
	balances, err := bc.fetchBalancesFromRedis()
	if err != nil {
		return err
	}

	bc.Lock()
	defer bc.Unlock()

	// Reset total balance
	bc.totalBalance = 0

	// Update balances
	for accountID, balance := range balances {
		bc.balances[accountID] = balance
		bc.totalBalance += balance
	}

	// Remove accounts that no longer exist
	for accountID := range bc.balances {
		if _, exists := balances[accountID]; !exists {
			delete(bc.balances, accountID)
		}
	}

	bc.lastUpdate = time.Now()
	return nil
}

// getBalance returns the balance and proportion for an account
func (bc *balanceCache) getBalance(accountID string) (float64, float64) {
	bc.RLock()
	defer bc.RUnlock()

	balance := bc.balances[accountID]
	proportion := 0.0
	if bc.totalBalance > 0 {
		proportion = balance / bc.totalBalance
	}
	return balance, proportion
}

// fetchBalancesFromRedis is a placeholder for actual Redis implementation
func (bc *balanceCache) fetchBalancesFromRedis() (map[string]float64, error) {
	fmt.Println("fetchBalancesFromRedis")
	// Implement based on your Redis schema
	balances := map[string]float64{
		"sam":   5.0,
		"shane": 6.0,
	}
	return balances, nil
}

type BaseWaitQueue struct {
	mu   sync.RWMutex
	n    int
	max  int
	last time.Time
	head *WaitingRequest
	tail *WaitingRequest
}

// DRRWaitQueue implements WaitQueue using Deficit Round Robin scheduling
type DRRWaitQueue struct {
	BaseWaitQueue

	// DRR scheduling state
	accounts    map[string]*accountState
	quantum     int64
	totalWeight int64
	bcache      *balanceCache
}

// NewDRRWaitQueue creates a new DRR-based wait queue with specified maximum capacity
func NewDRRWaitQueue(max int) *DRRWaitQueue {
	wq := &DRRWaitQueue{
		BaseWaitQueue: BaseWaitQueue{
			max: max,
		},
		accounts: make(map[string]*accountState),
		quantum:  1000, // Base quantum for granular scheduling
		bcache:   getBalanceCache(),
	}

	// Start weight updater
	go wq.weightUpdater()

	return wq
}

// weightUpdater periodically updates weights based on current balances
func (wq *DRRWaitQueue) weightUpdater() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		wq.updateWeights()
	}
}

// updateWeights updates the DRR weights based on current balances
func (wq *DRRWaitQueue) updateWeights() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	// Reset total weight
	wq.totalWeight = 0

	// Update weights for all accounts that have requests
	current := wq.head
	for current != nil {
		accountID := current.acc.Name
		balance, _ := wq.bcache.getBalance(accountID)

		state, exists := wq.accounts[accountID]
		if !exists {
			state = &accountState{}
			wq.accounts[accountID] = state
		}

		// Update weight
		state.weight = int64(balance * float64(wq.quantum))
		wq.totalWeight += state.weight

		current = current.next
	}

	// Clean up accounts that no longer have requests
	for accountID, state := range wq.accounts {
		if state.weight == 0 {
			delete(wq.accounts, accountID)
		}
	}
}

// Add implements WaitQueue.Add
func (wq *DRRWaitQueue) Add(wr *WaitingRequest) error {
	fmt.Println("Add", wr.reply)
	if wq == nil {
		return ErrWaitQueueNil
	}
	if wq.IsFull() {
		return ErrWaitQueueFull
	}

	wq.mu.Lock()
	defer wq.mu.Unlock()

	// Initialize account state if needed
	if _, exists := wq.accounts[wr.acc.Name]; !exists {
		wq.accounts[wr.acc.Name] = &accountState{}
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

// selectNextAccount uses DRR to select the next account to service
func (wq *DRRWaitQueue) selectNextAccount() string {
	if len(wq.accounts) == 0 {
		return ""
	}

	var selectedID string
	var minDeficit int64 = math.MaxInt64

	// Select account with lowest deficit
	for accountID, state := range wq.accounts {
		if state.deficit < minDeficit {
			minDeficit = state.deficit
			selectedID = accountID
		}
	}

	if selectedID == "" {
		return ""
	}

	// Increase deficit by totalWeight / weight for the selected account
	if state := wq.accounts[selectedID]; state.weight > 0 {
		state.deficit += wq.totalWeight / state.weight
	}

	return selectedID
}

// findNextRequestForAccount finds the next request for the given account
func (wq *DRRWaitQueue) findNextRequestForAccount(accountID string) *WaitingRequest {
	if accountID == "" {
		return nil
	}

	current := wq.head
	for current != nil {
		if current.acc.Name == accountID {
			return current
		}
		current = current.next
	}

	// Reset deficit if no request found
	if state, exists := wq.accounts[accountID]; exists {
		state.deficit = 0
	}

	return nil
}

// Peek implements WaitQueue.Peek
func (wq *DRRWaitQueue) Peek() *WaitingRequest {
	fmt.Println("Peek")
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.IsEmpty() {
		return nil
	}

	accountID := wq.selectNextAccount()
	if accountID == "" {
		return nil
	}

	return wq.findNextRequestForAccount(accountID)
}

// Tail implements WaitQueue.Tail
func (wq *DRRWaitQueue) Tail() *WaitingRequest {
	fmt.Println("Tail")
	return wq.tail
}

// Pop implements WaitQueue.Pop
func (wq *DRRWaitQueue) Pop() *WaitingRequest {
	fmt.Println("Pop")
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.IsEmpty() {
		return nil
	}

	accountID := wq.selectNextAccount()
	if accountID == "" {
		return nil
	}

	wr := wq.findNextRequestForAccount(accountID)
	if wr == nil {
		return nil
	}

	wr.d++
	wr.n--

	if wr.n > 0 && wq.n > 1 {
		wq.RemoveCurrent()
		wq.Add(wr)
	} else if wr.n <= 0 {
		wq.RemoveCurrent()
	}

	return wr
}

// Cycle implements WaitQueue.Cycle
func (wq *DRRWaitQueue) Cycle() {
	fmt.Println("Cycle")
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.IsEmpty() || wq.n == 1 {
		return
	}

	wr := wq.head
	if wr == nil {
		return
	}

	wq.RemoveCurrent()
	wq.Add(wr)

	// Reset deficit for this account
	if state, exists := wq.accounts[wr.acc.Name]; exists {
		state.deficit = 0
	}
}

// IsFull implements WaitQueue.IsFull
func (wq *DRRWaitQueue) IsFull() bool {
	fmt.Println("IsFull")
	if wq == nil {
		return false
	}
	return wq.n == wq.max
}

// IsEmpty implements WaitQueue.IsEmpty
func (wq *DRRWaitQueue) IsEmpty() bool {
	fmt.Println("IsEmpty")
	if wq == nil {
		return true
	}
	return wq.n == 0
}

// Len implements WaitQueue.Len
func (wq *DRRWaitQueue) Len() int {
	fmt.Println("Len")
	if wq == nil {
		return 0
	}
	return wq.n
}

func (wq *DRRWaitQueue) RemoveCurrent() {
	fmt.Println("RemoveCurrent")
	wq.Remove(nil, wq.head)
}

func (wq *DRRWaitQueue) Remove(pre, wr *WaitingRequest) {
	fmt.Println("Remove")
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
	wq.n--
}

func (wq *DRRWaitQueue) Last() time.Time {
	fmt.Println("Last")
	return wq.last
}

// SetLast implements WaitQueue.SetLast
func (wq *DRRWaitQueue) SetLast(t time.Time) {
	fmt.Println("SetLast")
	wq.last = t
}

// FIFOWaitQueue implements WaitQueue using simple FIFO ordering
type FIFOWaitQueue struct {
	BaseWaitQueue
}

// NewFIFOWaitQueue creates a new FIFO-based wait queue
func NewFIFOWaitQueue(max int) *FIFOWaitQueue {
	wq := &FIFOWaitQueue{
		BaseWaitQueue: BaseWaitQueue{
			max: max,
		},
	}
	return wq
}

// Add implements WaitQueue.Add
func (wq *FIFOWaitQueue) Add(wr *WaitingRequest) error {
	if wq == nil {
		return ErrWaitQueueNil
	}
	if wq.IsFull() {
		return ErrWaitQueueFull
	}
	if wq.head == nil {
		wq.head = wr
	} else {
		wq.tail.next = wr
	}
	// Always set tail.
	wq.tail = wr
	// Make sure nil
	wr.next = nil

	// Track last active via when we receive a request.
	wq.last = wr.received
	wq.n++
	return nil
}

// IsFull implements WaitQueue.IsFull
func (wq *FIFOWaitQueue) IsFull() bool {
	if wq == nil {
		return false
	}
	return wq.n == wq.max
}

// IsEmpty implements WaitQueue.IsEmpty
func (wq *FIFOWaitQueue) IsEmpty() bool {
	if wq == nil {
		return true
	}
	return wq.n == 0
}

// Len implements WaitQueue.Len
func (wq *FIFOWaitQueue) Len() int {
	if wq == nil {
		return 0
	}
	return wq.n
}

// Peek implements WaitQueue.Peek
func (wq *FIFOWaitQueue) Peek() *WaitingRequest {
	if wq == nil {
		return nil
	}
	return wq.head
}

// Tail implements WaitQueue.Tail
func (wq *FIFOWaitQueue) Tail() *WaitingRequest {
	if wq == nil {
		return nil
	}
	return wq.tail
}

// Cycle implements WaitQueue.Cycle
func (wq *FIFOWaitQueue) Cycle() {
	wr := wq.Peek()
	if wr != nil {
		// Always remove current now on a pop, and move to end if still valid.
		// If we were the only one don't need to remove since this can be a no-op.
		wq.RemoveCurrent()
		wq.Add(wr)
	}
}

// Pop implements WaitQueue.Pop
func (wq *FIFOWaitQueue) Pop() *WaitingRequest {
	wr := wq.Peek()
	if wr != nil {
		wr.d++
		wr.n--
		// Always remove current now on a pop, and move to end if still valid.
		// If we were the only one don't need to remove since this can be a no-op.
		if wr.n > 0 && wq.n > 1 {
			wq.RemoveCurrent()
			wq.Add(wr)
		} else if wr.n <= 0 {
			wq.RemoveCurrent()
		}
	}
	return wr
}

// RemoveCurrent implements WaitQueue.RemoveCurrent
func (wq *FIFOWaitQueue) RemoveCurrent() {
	wq.Remove(nil, wq.head)
}

// Remove implements WaitQueue.Remove
func (wq *FIFOWaitQueue) Remove(pre, wr *WaitingRequest) {
	if wr == nil {
		return
	}
	if pre != nil {
		pre.next = wr.next
	} else if wr == wq.head {
		// We are removing head here.
		wq.head = wr.next
	}
	// Check if wr was our tail.
	if wr == wq.tail {
		// Check if we need to assign to pre.
		if wr.next == nil {
			wq.tail = pre
		} else {
			wq.tail = wr.next
		}
	}
	wq.n--
}

func (wq *FIFOWaitQueue) Last() time.Time {
	return wq.last
}

// SetLast implements WaitQueue.SetLast
func (wq *FIFOWaitQueue) SetLast(t time.Time) {
	wq.last = t
}

type WaitQueueInfo struct {
	wq     WaitQueue
	stream string
}

// NewWaitQueue creates a new wait queue of the specified type
func NewWaitQueue(max int, stream string) WaitQueue {
	fmt.Println("NewWaitQueue:", stream)

	waitQueueMutex.Lock()
	defer waitQueueMutex.Unlock()

	// Check if queue already exists for this stream
	if info, exists := waitQueueMap[stream]; exists {
		return info.wq
	}

	isInference := strings.Contains(stream, "inference") || strings.Contains(stream, "Inference")

	var wq WaitQueue
	switch isInference {
	case true:
		wq = NewDRRWaitQueue(max)
	default:
		wq = NewFIFOWaitQueue(max)
	}

	// Create new WaitQueueInfo and store in map
	info := &WaitQueueInfo{
		wq:     wq,
		stream: stream,
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
			for _, info := range waitQueueMap {
				fmt.Printf("%d:%s\n", info.wq.Len(), info.stream)
			}
		}
	}()
}
