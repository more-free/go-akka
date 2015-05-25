package akka

import (
	"fmt"
)

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

type ActorPath []string

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

// provided as a parent object for user-created actors
// it (intentionally) only implements partial methods of actor interfaces.
// Specifically, any user-created actor must implement receive(Message),
// and must be created from ActorSystem to get context() method overwritten.
type DefaultActor struct {
	NullBehavior
	NullLifeCycle
	NullSupervisorStrategy
}

type NullBehavior struct{}

// provided by user. must be overwritten
func (this *NullBehavior) receive(msg Message) {
	panic("receive(Message) must be implemented !")
}

type NullSupervisorStrategy struct{}

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

type DefaultActorContext struct {
	_parent   ActorRef
	_children []ActorRef
	_props    Props
	_self     ActorRef
	_sender   ActorRef
	_system   ActorSystem
}

func (this *DefaultActorContext) parent() ActorRef {
	return this._parent
}

func (this *DefaultActorContext) children() []ActorRef {
	return this._children
}

func (this *DefaultActorContext) setParent(parent ActorRef) {
	this._parent = parent
}

func (this *DefaultActorContext) addChild(child ActorRef) {
	this._children = append(this._children, child)
}

func (this *DefaultActorContext) self() ActorRef {
	return this._self
}

func (this *DefaultActorContext) sender() ActorRef {
	return this._sender
}

func (this *DefaultActorContext) system() ActorSystem {
	return this._system
}

func (this *DefaultActorContext) setSystem(as ActorSystem) {
	this._system = as
}

func (this *DefaultActorContext) props() Props {
	return this._props
}

func (this *DefaultActorContext) dispatcher() Dispatcher {
	return this.system().dispatcher()
}

func (this *DefaultActorContext) actorOf(props Props, name string) ActorRef {
	/*
		// create the actor instance
		actor := props.build()

		// combine with basic actor Behaivor

		// overwrite its context
		actor.setContext(this.createContext()) // TODO

		// add the ActorSystem. after this, the actor is ready in the
		// dispatcher of the actor system, but since its ActorRef has
		// not been returned to the caller, there is no way to send message
		// to the actor. So it is safe to invoke preStart hook after adding
		// it to actor system.
		actorRef, _ := actor.context().system().put(actor, []string{name})

		// add the newly created actor as children
		this.addChild(actorRef)

		// call preStart hook
		actor.preStart()

		// return to caller and be ready to receive messages
		return actorRef
	*/
	return nil
}

func (this *DefaultActorContext) createContext() ActorContext {
	cxt := new(DefaultActorContext)
	cxt.setParent(this._self)
	// other fields get zero values

	return cxt
}

func (this *DefaultActorContext) actorFor(path string) ActorRef {
	return nil
}

func (this *DefaultActorContext) stop(actor ActorRef) {

}

type ActorEvent interface {
	eventType() ActorEventType
}

type ActorEventType int

const (
	ADD = iota
	REMOVE
)

type ActorEventListener interface {
	handle(event ActorEvent)
}
