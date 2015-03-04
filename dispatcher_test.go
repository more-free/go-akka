package akka

import (
	"strconv"
	"strings"
	"testing"
)

// TODO test OneForMoreActorRuntime(Pool)
// TODO test SharedMessageQueue
// TODO test OneForOneMessageQueue
// TODO test Dispatcher

func TestSharedMessageQueue(t *testing.T) {
	// TODO
}

func TestDefaultSingleMQ(t *testing.T) {
	mq := DefaultMQBuilder() // returns a DefaultSingleMQ

	// test basic function
	mq.offer(&msg{"m1"})
	mq.offer(&msg{"m2"})
	assertIntEquals(t, 2, mq.size())
	m, ok := mq.poll()
	assertBoolEquals(t, true, ok)
	assertStringEquals(t, "m1", m.id())
	m, ok = mq.poll()
	assertBoolEquals(t, true, ok)
	assertStringEquals(t, "m2", m.id())
	assertIntEquals(t, 0, mq.size())

	// offer and poll should be thread safe
	ready := make(chan bool)
	for i := 0; i < 100; i += 1 {
		go func(mq SingleMQ, ready chan bool, i int) {
			mq.offer(&msg{"m" + strconv.Itoa(i)})
			ready <- true
		}(mq, ready, i)
	}

	get := 0
	for {
		<-ready
		get += 1
		if get == 100 {
			break
		}
	}

	assertIntEquals(t, 100, mq.size())

	msgs := make(map[string]bool)
	for mq.size() > 0 {
		if m, ok = mq.poll(); ok {
			msgs[m.id()] = true
		}
	}
	assertIntEquals(t, 100, len(msgs))
}

func TestDefaultActorRuntime(t *testing.T) {
	echor := new(echor)
	echor.received = make(chan Message, 3)
	rt := NewDefaultActorRuntime()

	ms := []Message{&msg{"m1"}, &msg{"m2"}, &msg{"m3"}}
	for _, m := range ms {
		rt.receive(m, echor)
	}

	rt.stop()

	assertStringEquals(t, "m1", (<-echor.received).id())
	assertStringEquals(t, "m2", (<-echor.received).id())
	assertStringEquals(t, "m3", (<-echor.received).id())
}

func TestOneForOneActorRuntimePool(t *testing.T) {
	rtp := NewOneForOneActorRuntimePool()

	actor1, actor2 := new(echor), new(echor)
	actor1.received = make(chan Message, 2)
	actor2.received = make(chan Message, 2)

	rtp.add(actor1)
	rtp.add(actor2)

	ms := []Message{&msg{"a1m1"}, &msg{"a2m1"}, &msg{"a1m2"}, &msg{"a2m2"}}
	nextActor := actor1
	for _, m := range ms {
		rtp.receive(m, nextActor)
		if nextActor == actor1 {
			nextActor = actor2
		} else {
			nextActor = actor1
		}
	}

	rtp.shutdown()

	assertStringEquals(t, "a1m1", (<-actor1.received).id())
	assertStringEquals(t, "a1m2", (<-actor1.received).id())
	assertStringEquals(t, "a2m1", (<-actor2.received).id())
	assertStringEquals(t, "a2m2", (<-actor2.received).id())
}

// helpers
type echor struct {
	received chan Message
}

func (this *echor) receive(msg Message) {
	this.received <- msg
}

type msg struct {
	name string
}

func (this *msg) id() string {
	return this.name
}

func assertIntEquals(t *testing.T, expected, actual int) {
	if expected != actual {
		t.Error("expected = " + strconv.Itoa(expected) +
			", actual = " + strconv.Itoa(actual))
	}
}

func assertBoolEquals(t *testing.T, expected, actual bool) {
	if expected != actual {
		t.Error("expected = " + strconv.FormatBool(expected) +
			", actual = " + strconv.FormatBool(actual))
	}
}

func assertStringEquals(t *testing.T, expected, actual string) {
	if !strings.EqualFold(expected, actual) {
		t.Error("expected = " + expected + ", actual = " + actual)
	}
}
