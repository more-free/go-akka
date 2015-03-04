package akka

import (
	"fmt"
	"testing"
)

func TestDefaultActorRuntime(t *testing.T) {
	echor := new(echor)
	rt := NewDefaultActorRuntime()

	ms := []Message{&msg{"m1"}, &msg{"m2"}, &msg{"m3"}}
	for _, m := range ms {
		rt.receive(m, echor)
	}

	rt.stop()
}

func TestOneForOneActorRuntimePool(t *testing.T) {
	rtp := NewOneForOneActorRuntimePool()

	actor1, actor2 := new(echor), new(echor)
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
}

// helpers
type echor struct{}

func (this *echor) receive(msg Message) {
	fmt.Println(msg.id())
}

type msg struct {
	name string
}

func (this *msg) id() string {
	return this.name
}
