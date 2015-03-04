package akka

type Message interface {
	id() string
}

type Actor interface {
	receive(msg Message)
	//context() ActorContext
}

type ActorContext interface {
}

type ActorMessageQueue interface {
	// block until the next message is available
	nextMessage() (Message, Actor)
}

type ActorRuntimePool interface {
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

	freeWorkers chan ActorRuntime
	workers     []ActorRuntime
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
			next := <-rt.channel
			if next.msg != nil && next.actor != nil {
				rt.receive(next.msg, next.actor)
			}

			// notify runtime pool "I am free", and wait for the message
			rt.freeWorkers <- rt
		}
	}(rt)

	return rt
}

func NewOneForMulActorRuntimePool(maxStandings int) ActorRuntimePool {
	rtp := new(OneForMulActorRuntimePool)
	rtp.maxStandings = maxStandings
	rtp.actors = make(map[Actor]bool)
	rtp.workers = make([]ActorRuntime, maxStandings/2+1)
	rtp.freeWorkers = make(chan ActorRuntime)

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
		this.actors[actor] = true
		if len(this.actors) < this.maxStandings {
			// create a new worker
			this.workers = append(this.workers, NewSharedActorRuntime(this.freeWorkers))
		}
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

type Dispatcher struct {
	messageQueue ActorMessageQueue
	runtime      ActorRuntimePool
	stop         bool
}

func (this *Dispatcher) start() {
	for !this.stop {
		msg, actor := this.messageQueue.nextMessage()
		this.runtime.receive(msg, actor)
	}
}
