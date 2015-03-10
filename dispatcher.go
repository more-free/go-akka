package akka

import (
	"container/list"
	"strings"
	"sync"
	"fmt"
)

type Message interface {
	id() string
}

// all methods in Actor are invisiable to outside world
// there is no way to change an actor's internal state
// except by passing messages through its ActorRef
// TODO must provide default implementation to all methods
// except receive(Message)
type Actor interface {
	Behaivor
	LifeCycle
	SupervisorStrategy
	
	context() ActorContext
	setContext(cxt ActorContext) 
}

type Behaivor interface {
	receive(msg Message)
}

type LifeCycle interface {
	// lifecycle hook
	preStart()
	preRestart()
	postRestart()
	preStop()
	postStop()
}

type SupervisorStrategy interface {
	// TODO add arguments for the method
	processFailure()
}


// provided as a parent object for user-created actors
// it (intentionally) only implements partial methods of actor interfaces.
// Specifically, any user-created actor must implement receive(Message),
// and must be created from ActorSystem to get context() method overwritten.
type DefaultActor struct {
	NullBehavior
	NullLifeCycle
	NullSupervisorStrategy
}

type NullBehavior struct { }

// provided by user. must be overwritten
func (this *NullBehavior) receive(msg Message) {
	panic("receive(Message) must be implemented !")
}

type NullSupervisorStrategy struct { }

// can be empty.
func (this *NullSupervisorStrategy) processFailure() {
	panic("An actor must provide non-empty supervisor strategy")
}

type NullLifeCycle struct{}

func (this *NullLifeCycle) preStart()    {}
func (this *NullLifeCycle) preRestart()  {}
func (this *NullLifeCycle) postRestart() {}
func (this *NullLifeCycle) preStop()     {}
func (this *NullLifeCycle) postStop()    {}

// provided by ActorSystem. must be overwritten
func (this *DefaultActor) context() ActorContext {
	panic("An actor must provide ActorContext")
}

// provided by ActorSystem. must be overwritten
func (this *DefaultActor) setContext(cxt ActorContext) {
	panic("An actor must privide setContext(ActorContext)")
}

// TODO for test purpose only
type SimpleActor struct {
	DefaultActor
}

func (this *SimpleActor) receive(msg Message) {
	fmt.Println(msg.id())
}

// hold an actor implicitly
// the main purpose of ActorRef is, location transparency and auto failover
type ActorRef interface {
	path() ActorPath
	tell(msg Message) // send message to the actor it represents
	forward(msg Message, sender ActorRef)
	
	// TODO should this context field private
	// ActorRef and Actor must point to the same ActorContext 
	// context() ActorContext 
	
	// compareTo, equals, hashCode, toString
}

type ActorPath interface {
}

// TODO explore some reflection solution in go 
type Props interface {
	build() Actor
}

type ActorContext interface {
	// create an child actor and add it to ActorSystem
	actorOf(props Props, name string) ActorRef
	dispatcher() Dispatcher
	parent() ActorRef
	children() []ActorRef
	props() Props
	self() ActorRef
	sender() ActorRef // ref to the sender of the last message
	system() ActorSystem
	stop(actor ActorRef)
	
	// TODO
	// become(behaivor Behaivor, discardOld bool)
	// unbecome()
	// watch(subject ActorRef)
	// unwatch(subject ActorRef)

	actorFor(path string) ActorRef // look for actor given a path
}

type DefaultActorContext struct {
	parent ActorRef
	children []ActorRef
	props Props
	self ActorRef
	sender ActorRef
	system ActorSystem
}

func (this *DefaultActorContext) parent() ActorRef {
	return this.parent
}

func (this *DefaultActorContext) children []ActorRef {
	return this.children
}

func (this *DefaultActorContext) setParent(parent ActorRef) {
	this.parent = parent
}

func (this *DefaultActorContext) addChild(child ActorRef) {
	this.children = append(this.children, child)
} 

func (this *DefaultActorContext) self() ActorRef {
	return this.self
}

func (this *DefaultActorContext) sender() ActorRef {
	return this.sender
}

func (this *DefaultActorContext) system() ActorRef {
	return this.system
}

func (this *DefaultActorContext) setSystem(as ActorSystem) {
	this.system = as
}

func (this *DefaultActorContext) props() Props {
	return this.props
}

func (this *DefaultActorContext) Dispatcher {
	return this.system().getDispatcher()
}

func (this *DefaultActorContext) actorOf(props Props, name string) ActorRef {
	// create the actor instance
	actor := props.build()
	
	// overwrite its context
	actor.setContext(this.createContext())  // TODO
	
	// add the ActorSystem. after this, the actor is ready in the 
	// dispatcher of the actor system, but since its ActorRef has 
	// not been returned to the caller, there is no way to send message
	// to the actor. So it is safe to invoke preStart hook after adding 
	// it to actor system.  
	actorRef := actor.context().system().add(actor, name)
	
	// add the newly created actor as children
	this.addChild(actorRef)
	
	// call preStart hook
	actor.preStart()
	
	// return to caller and be ready to receive messages
	return actorRef
}

func (this *DefaultActorContext) createContext() ActorContext {
	cxt := new(DefaultActorContext)
	cxt.setParent(this.self)
	// other fields get zero values
	
	return cxt
}

func (this *DefaultActorContext) 

type ActorSystem interface {
	// TODO should keep all methods that directly interact with actors private
	add(actor Actor, name string) ActorRef
	remove(actor Actor)
	
	actorFor(path string) ActorRef

	// for actor-related events(add, remove, etc.), registered listeners
	// should be invoked synchronously
	addListener(listener ActorEventListener)
	removeListener(listener ActorEventListener)

	bind(dispatcher Dispatcher)
	getDispatcher() Dispatcher
}

type ActorEvent interface {
	eventType() ActorEventType
}

type ActorEventType int

const (
	_ ActorEventType = iota
	ADD
	REMOVE
)

const namesActorEventType = []string{
	ADD : "ADD"
	REMOVE : "REMOVE"
}

type ActorEventListener interface {
	handle(event ActorEvent)
}

// TODO separate queue structure (one shared queue, multiple queues, etc.)
// from the selection strategy (how to enqueue, how to dequeue, how to
// attain a fair schedule)
type ActorMessageQueue interface {
	// block until the next message is available
	poll() (Message, Actor)

	// block for shared queue, non-blocking for one for one queue
	offer(msg Message, actor Actor)
}

// a single thread-safe queue shared by all actors
type SharedMessageQueue struct {
	bufferSize int
	channel    chan *ActorRuntimeMessage
}

func NewSharedMessageQueue(bufferSize int) ActorMessageQueue {
	mq := new(SharedMessageQueue)
	mq.bufferSize = bufferSize
	mq.channel = make(chan *ActorRuntimeMessage, bufferSize)
	return mq
}

func (this *SharedMessageQueue) poll() (Message, Actor) {
	next := <-this.channel
	return next.msg, next.actor
}

func (this *SharedMessageQueue) offer(msg Message, actor Actor) {
	this.channel <- &ActorRuntimeMessage{msg, actor}
}

type OneForOneMessageQueue struct {
	mqs       map[Actor]SingleMQ // TODO better use a LinkedHashMap
	mqBuilder func() SingleMQ

	ready   chan Actor
	waiting chan bool

	nextMsg   func() (Message, Actor, bool) // helper generator
	processed int
	mutex     *sync.Mutex
}

func (this *OneForOneMessageQueue) isWaiting() bool {
	select {
	case <-this.waiting:
		return true
	default:
		return false
	}
}

// helper type for OneForOneMessageQueue, use my existing implementation
// must be thread-safe : offer() and poll() might run concurrently
type SingleMQ interface {
	offer(msg Message)
	poll() (Message, bool) // must be non-blocking
	size() int
}

func DefaultMQBuilder() SingleMQ {
	return &DefaultSingleMQ{new(sync.Mutex), list.New()}
}

type DefaultSingleMQ struct {
	mutex *sync.Mutex
	queue *list.List
}

func (this *DefaultSingleMQ) offer(msg Message) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.queue.PushBack(msg)
}

func (this *DefaultSingleMQ) poll() (Message, bool) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if this.queue.Len() == 0 {
		return nil, false
	} else {
		front := this.queue.Front()
		this.queue.Remove(front)
		msg, ok := front.Value.(Message)
		return msg, ok
	}
}

func (this *DefaultSingleMQ) size() int {
	return this.queue.Len()
}

func NewOneForOneMessageQueue() ActorMessageQueue {
	mq := new(OneForOneMessageQueue)
	mq.mqs = make(map[Actor]SingleMQ)
	mq.mqBuilder = DefaultMQBuilder
	mq.ready = make(chan Actor)
	mq.waiting = make(chan bool)
	mq.nextMsg = nil
	mq.processed = 0
	mq.mutex = new(sync.Mutex)

	return mq
}

func NewOneForOneMessageQueueWithBuilder(mqBuilder func() SingleMQ) ActorMessageQueue {
	mq := new(OneForOneMessageQueue)
	mq.mqs = make(map[Actor]SingleMQ)
	mq.mqBuilder = mqBuilder
	mq.waiting = make(chan bool)
	mq.ready = make(chan Actor)
	mq.nextMsg = nil
	mq.processed = 0
	mq.mutex = new(sync.Mutex)

	return mq
}

func (this *OneForOneMessageQueue) poll() (Message, Actor) {
	if this.nextMsg == nil {
		this.nextMsg = this.next()
	}

	for {
		msg, actor, ok := this.nextMsg()
		if ok {
			this.processed += 1
			return msg, actor
		} else {
			if this.processed > 0 {
				this.processed = 0
				this.nextMsg = this.next()
			} else {
				// to check if there is really no message
				this.mutex.Lock()
				size := this.size()
				if size > 0 {
					this.mutex.Unlock()
					continue
				} else {
					// block until any channel is ready
					this.waiting <- true
					this.mutex.Unlock()

					<-this.ready
					this.processed = 0
					this.nextMsg = this.next()
				}
			}
		}
	}
}

// total size of messages (for now)
func (this *OneForOneMessageQueue) size() int {
	s := 0
	for _, mq := range this.mqs {
		s += mq.size()
	}
	return s
}

// a helper function for poll(). a generator
func (this *OneForOneMessageQueue) next() func() (Message, Actor, bool) {
	actors := make([]Actor, len(this.mqs))
	mqs := make([]SingleMQ, len(this.mqs))
	i := 0
	for actor, mq := range this.mqs {
		actors[i] = actor
		mqs[i] = mq
		i += 1
	}

	j := -1
	return func() (Message, Actor, bool) {
		for j += 1; j <= i-1; j += 1 {
			m, ok := mqs[j].poll()
			if ok {
				return m, actors[j], true
			}
		}
		return nil, nil, false
	}
}

// non-blocking, no-boundary
func (this *OneForOneMessageQueue) offer(msg Message, actor Actor) {
	if mq, exists := this.mqs[actor]; !exists {
		q := this.mqBuilder()
		q.offer(msg)
		this.mqs[actor] = q
	} else {
		mq.offer(msg)
	}

	this.notify(actor)
}

func (this *OneForOneMessageQueue) notify(actor Actor) {
	if this.isWaiting() {
		this.ready <- actor
	}
}

type ActorRuntimePool interface {
	// invoke actor's receive(Message) inside the runtime
	// an Actor must be added by invoking add() first
	receive(msg Message, actor Actor)

	add(actor Actor)
	remove(actor Actor)
	shutdown()
	// TODO add more lifecycle hook
}

// providing one-to-one mapping between actor and go routine
type OneForOneActorRuntimePool struct {
	runtimePool map[Actor]ActorRuntime
}

func NewOneForOneActorRuntimePool() ActorRuntimePool {
	return &OneForOneActorRuntimePool{make(map[Actor]ActorRuntime)}
}

// an Actor must be added into runtime pool first before receiving messages
func (this *OneForOneActorRuntimePool) receive(msg Message, actor Actor) {
	if rt, ok := this.runtimePool[actor]; ok {
		rt.receive(msg, actor)
	} else {
		panic("actor was not in the runtime pool. Use add(Actor) to add new actors first")
	}
}

func (this *OneForOneActorRuntimePool) add(actor Actor) {
	if _, ok := this.runtimePool[actor]; !ok {
		this.runtimePool[actor] = NewDefaultActorRuntime()
	}
}

func (this *OneForOneActorRuntimePool) remove(actor Actor) {
	if _, ok := this.runtimePool[actor]; ok {
		delete(this.runtimePool, actor)
	}
}

func (this *OneForOneActorRuntimePool) shutdown() {
	for _, rt := range this.runtimePool {
		rt.stop()
	}
}

// shared runtime pool implementation
type OneForMulActorRuntimePool struct {
	// number of active workers = min(len(actors), maxStandings)
	maxStandings int            // max number of allowed go routines
	actors       map[Actor]bool // use as an known actor set

	freeWorkers   chan ActorRuntime
	workers       []ActorRuntime
	activeWorkers int
}

type SharedActorRuntime struct {
	channel chan *ActorRuntimeMessage // process message from actors

	// write back self status when it becomes available. notify runtime pool
	// to schedule self in the future
	freeWorkers chan ActorRuntime
}

func (this *SharedActorRuntime) receive(msg Message, actor Actor) {
	this.channel <- &ActorRuntimeMessage{msg, actor}
}

func (this *SharedActorRuntime) stop() {
	this.channel <- &ActorRuntimeMessage{nil, nil}
}

func NewSharedActorRuntime(freeWorkers chan ActorRuntime) ActorRuntime {
	rt := &SharedActorRuntime{
		make(chan *ActorRuntimeMessage),
		freeWorkers,
	}

	go func(rt *SharedActorRuntime) {
		for {
			// notify runtime pool "I am free", and wait for being invoked
			rt.freeWorkers <- rt

			// wait for the next message
			next := <-rt.channel
			if next.msg != nil && next.actor != nil {
				next.actor.receive(next.msg)
			}
		}
	}(rt)

	return rt
}

func NewOneForMulActorRuntimePool(maxStandings int) ActorRuntimePool {
	rtp := new(OneForMulActorRuntimePool)
	rtp.maxStandings = maxStandings
	rtp.actors = make(map[Actor]bool)
	rtp.workers = make([]ActorRuntime, maxStandings)
	rtp.activeWorkers = 0
	rtp.freeWorkers = make(chan ActorRuntime, maxStandings)

	return rtp
}

// block until at least one worker is available
func (this *OneForMulActorRuntimePool) receive(msg Message, actor Actor) {
	// select free worker from freeWorkers chan
	if _, exists := this.actors[actor]; exists {
		worker := <-this.freeWorkers
		worker.receive(msg, actor)
	} else {
		panic("actor was not in the runtime pool. Use add(Actor) to add new actors first")
	}
}

func (this *OneForMulActorRuntimePool) add(actor Actor) {
	if _, exists := this.actors[actor]; !exists {
		if this.activeWorkers < this.maxStandings {
			// create a new worker
			this.workers[this.activeWorkers] = NewSharedActorRuntime(this.freeWorkers)
			this.activeWorkers += 1
		}
		this.actors[actor] = true
	}
}

func (this *OneForMulActorRuntimePool) remove(actor Actor) {
	if _, exists := this.actors[actor]; exists {
		delete(this.actors, actor)
	}
}

func (this *OneForMulActorRuntimePool) shutdown() {
	for _, rt := range this.workers {
		rt.stop()
	}
}

// single actor runtime object
type ActorRuntime interface {
	receive(msg Message, actor Actor)
	stop()
	// TODO add more lifecycle hook
}

// internal message
type ActorRuntimeMessage struct {
	msg   Message
	actor Actor
}

// go routine based runtime, with default buffer size 1
type DefaultActorRuntime struct {
	channel chan *ActorRuntimeMessage
}

func NewDefaultActorRuntime() ActorRuntime {
	rt := &DefaultActorRuntime{make(chan *ActorRuntimeMessage)}

	go func(rt *DefaultActorRuntime) {
		for {
			next := <-rt.channel

			// quit if it is stop message
			if next.actor == nil && next.msg == nil {
				break
			} else {
				next.actor.receive(next.msg)
			}
		}
	}(rt)

	return rt
}

func (this *DefaultActorRuntime) receive(msg Message, actor Actor) {
	this.channel <- &ActorRuntimeMessage{msg, actor}
}

func (this *DefaultActorRuntime) stop() {
	this.channel <- &ActorRuntimeMessage{nil, nil}
}

type Dispatcher interface {
	// TODO add more APIs
	register(actor Actor)
	unregister(actor Actor)
	offer(msg Message, actor Actor) // usually non-blocking
	start()                         // block current thread
	stop()
}

// an experimental dispatcher for demo purpose
type DefaultDispatcher struct {
	messageQueue ActorMessageQueue
	runtimePool  ActorRuntimePool

	stateTransfer chan string // TODO add a complete FSM
	state         string
}

func NewDefaultDispatcher() Dispatcher {
	return &DefaultDispatcher{
		NewSharedMessageQueue(1024 * 5),
		NewOneForMulActorRuntimePool(500),
		make(chan string),
		"init",
	}
}

func (this *DefaultDispatcher) register(actor Actor) {
	this.runtimePool.add(actor)
}

func (this *DefaultDispatcher) unregister(actor Actor) {
	this.runtimePool.remove(actor)
}

func (this *DefaultDispatcher) offer(msg Message, actor Actor) {
	this.messageQueue.offer(msg, actor)
}

func (this *DefaultDispatcher) start() {
	this.monitorState()
	this.state = "started"

	for !this.isStopped() {
		msg, actor := this.messageQueue.poll()
		this.runtimePool.receive(msg, actor)
	}
}

func (this *DefaultDispatcher) monitorState() {
	go func() {
		for {
			this.state = <-this.stateTransfer
			if this.isStopped() {
				break
			}
		}
	}()
}

func (this *DefaultDispatcher) stop() {
	this.stateTransfer <- "stop"
}

func (this *DefaultDispatcher) isStopped() bool {
	return strings.EqualFold("stopped", this.state)
}
