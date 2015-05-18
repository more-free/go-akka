package akka

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDefaultDispatcher(t *testing.T) {
	dis := NewDefaultDispatcher()
	sleeper1 := new(sleeper)
	sleeper1.sleepTime = 50 * time.Millisecond
	sleeper1.received = make(chan Message, 4)

	sleeper2 := new(sleeper)
	sleeper2.sleepTime = 50 * time.Millisecond
	sleeper2.received = make(chan Message, 4)

	dis.register(sleeper1)
	dis.register(sleeper2)

	go dis.start()
	time.Sleep(200 * time.Millisecond)

	msgsCnt := []string{"m1", "m2", "m3", "m4"}
	msgs := make([]Message, len(msgsCnt))
	for i, s := range msgsCnt {
		msgs[i] = &msg{s}
	}

	dis.offer(msgs[0], sleeper1)
	dis.offer(msgs[1], sleeper2)
	dis.offer(msgs[2], sleeper1)
	dis.offer(msgs[3], sleeper2)

	m := <-sleeper1.received
	assertStringEquals(t, "m1", m.id())
	m = <-sleeper1.received
	assertStringEquals(t, "m3", m.id())
	m = <-sleeper2.received
	assertStringEquals(t, "m2", m.id())
	m = <-sleeper2.received
	assertStringEquals(t, "m4", m.id())
}

func TestOneForMulActorRuntimePool(t *testing.T) {
	testForOneActor(t)
	testForTwoActors(t)
}

func testForOneActor(t *testing.T) {
	rtp := NewOneForMulActorRuntimePool(1)
	msgsCnt := []string{"m1", "m2", "m3", "m4"}
	msgs := make([]Message, len(msgsCnt))
	for i, s := range msgsCnt {
		msgs[i] = &msg{s}
	}

	sleeper1 := new(sleeper)
	sleeper1.sleepTime = 200 * time.Millisecond
	sleeper1.received = make(chan Message, 4)

	rtp.add(sleeper1)
	rtp.receive(msgs[0], sleeper1)

	isBlock := true
	blocking := make(chan bool)
	go func() {
		assertBoolEquals(t, true, isBlock)
		<-blocking
		assertBoolEquals(t, false, isBlock)
	}()

	// shoud get blocked because there is only one go routine inside runtime pool
	rtp.receive(msgs[1], sleeper1)
	isBlock = false
	blocking <- isBlock

	rtp.receive(msgs[2], sleeper1)
	rtp.receive(msgs[3], sleeper1)

	for i := 0; i < 4; i += 1 {
		m := <-sleeper1.received
		assertStringEquals(t, msgsCnt[i], m.id())
	}

	rtp.shutdown()
}

func testForTwoActors(t *testing.T) {
	rtp := NewOneForMulActorRuntimePool(2)
	msgsCnt := []string{"m1", "m2", "m3", "m4"}
	msgs := make([]Message, len(msgsCnt))
	for i, s := range msgsCnt {
		msgs[i] = &msg{s}
	}

	sharedReceived := make(chan Message, 4)
	sleeper1 := new(sleeper)
	sleeper1.sleepTime = 50 * time.Millisecond
	sleeper1.received = sharedReceived

	sleeper2 := new(sleeper)
	sleeper2.sleepTime = 150 * time.Millisecond
	sleeper2.received = sharedReceived

	rtp.add(sleeper1)
	rtp.add(sleeper2)
	rtp.receive(msgs[0], sleeper1)
	rtp.receive(msgs[1], sleeper2)

	isBlock := true
	blocking := make(chan bool)
	go func() {
		assertBoolEquals(t, true, isBlock)
		<-blocking
		assertBoolEquals(t, false, isBlock)
	}()

	rtp.receive(msgs[2], sleeper1)
	isBlock = false
	blocking <- isBlock

	rtp.receive(msgs[3], sleeper2)

	res := make(map[string]bool)
	for i := 0; i < len(msgs); i += 1 {
		m := <-sharedReceived
		res[m.id()] = true
	}

	// two sleepers should process all messages
	assertIntEquals(t, len(msgs), len(res))

	rtp.shutdown()
}

func TestOneForOneMessageQueue(t *testing.T) {
	actor1 := new(echor)
	actor1.received = make(chan Message, 2)

	actor2 := new(echor)
	actor2.received = make(chan Message, 2)

	mq := NewOneForOneMessageQueue() // with default SingeMQ builder
	mq.offer(&msg{"m1a1"}, actor1)
	mq.offer(&msg{"m2a1"}, actor2)

	msgs := make([]Message, 2)
	actors := make([]Actor, 2)

	m, a := mq.poll()
	msgs[0] = m
	actors[0] = a

	m, a = mq.poll()
	msgs[1] = m
	actors[1] = a

	// the received message might be out of order, but have to be consistent
	if strings.EqualFold(msgs[0].id(), "m1a1") {
		assertBoolEquals(t, true, actors[0] == actor1)
		assertStringEquals(t, "m2a1", msgs[1].id())
		assertBoolEquals(t, true, actors[1] == actor2)
	} else {
		assertStringEquals(t, "m2a1", msgs[0].id())
		assertBoolEquals(t, true, actors[0] == actor2)
		assertBoolEquals(t, true, actors[1] == actor1)
		assertStringEquals(t, "m1a1", msgs[1].id())
	}

	// should block
	isBlock := true

	go func() {
		time.Sleep(200 * time.Millisecond) // let poll() blocked
		assertBoolEquals(t, true, isBlock)
		mq.offer(&msg{"m2a1"}, actor1)
	}()

	m, a = mq.poll()
	isBlock = false
	assertStringEquals(t, "m2a1", m.id())
}

func TestSharedMessageQueue(t *testing.T) {
	// echor does nothing in this test
	echor := new(echor)
	echor.received = make(chan Message, 3)

	mq := NewSharedMessageQueue(3)
	mq.offer(&msg{"m1"}, echor)
	mq.offer(&msg{"m2"}, echor)
	mq.offer(&msg{"m3"}, echor)
	is4thMsgReady := false

	// the 4th message should be blocked until the one of the first three
	// is consumed
	go func() {
		mq.offer(&msg{"m4"}, echor)
		is4thMsgReady = true
	}()

	assertBoolEquals(t, false, is4thMsgReady)

	msg, _ := mq.poll()
	assertStringEquals(t, "m1", msg.id())

	msg, _ = mq.poll()
	assertStringEquals(t, "m2", msg.id())

	msg, _ = mq.poll()
	assertStringEquals(t, "m3", msg.id())

	msg, _ = mq.poll()
	assertStringEquals(t, "m4", msg.id())

	assertBoolEquals(t, true, is4thMsgReady)
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

// an empty struct that does nothing but implements Actor interface
// except the receive method. In other words, it is an "abstract" class
type baseActor struct {
	DefaultActor
}

func (this *baseActor) context() ActorContext {
	return nil
}

func (this *baseActor) setContext(cxt ActorContext) {

}

func enableFmtImport() {
	fmt.Println("")
}

type sleeper struct {
	baseActor
	sleepTime time.Duration
	received  chan Message
}

func (this *sleeper) receive(msg Message) {
	time.Sleep(this.sleepTime)
	this.received <- msg
}

type reverser struct {
	baseActor
	received chan string
}

func (this *reverser) receive(msg Message) {
	this.received <- reverse(msg.id())
}

// from https://github.com/golang/example/blob/master/stringutil/reverse.go
func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

type echor struct {
	baseActor
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
