// actor_system_test.go
package akka

import (
	"fmt"
	"testing"
)

func TestInMemTreeStore(t *testing.T) {
	s := NewDefaultTreeStore()
	unit := &actorUnit{new(DefaultActor), nil, ""}

	s.put(unit, []string{"hello", "golang", "akka"})
	s.put(unit, []string{"hello", "scala", "akka"})
	s.put(unit, []string{"hello", "golang", "spark"})
	_, err := s.get([]string{"hello", "golang", "akka"})
	if err != nil {
		t.Error(err.Error())
	}

	children, err := s.getChildren([]string{"hello"})
	if len(children) != 0 {
		t.Errorf("Children number %v doesn't match the real value 0", len(children))
	}

	children, err = s.getChildren([]string{"hello", "golang"})
	if len(children) != 2 {
		t.Errorf("Children number %v doesn't match the real value 2", len(children))
	}

	parent, err := s.getParent([]string{"hello", "scala", "akka"})
	if err != nil {
		t.Errorf(err.Error())
	}
	if parent != nil {
		t.Errorf("Parent of %v should be nil", []string{"hello", "scala", "akka"})
	}

	s.remove([]string{"hello", "golang", "spark"}, false)
	children, err = s.getChildren([]string{"hello", "golang"})
	if !(len(children) == 1) {
		t.Errorf("Removal causes error")
	}

	s.remove([]string{"hello", "golang"}, true)
	children, err = s.getChildren([]string{"hello", "scala"})
	if len(children) != 1 {
		t.Errorf("Removal causes error")
	}
}

func dummy() { fmt.Println() }
