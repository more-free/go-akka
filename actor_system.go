// actor_system.go
package akka

import (
	"fmt"
)

type ActorBuilder func() Actor

type ActorSystem interface {
	TreeStore

	// TODO
	// for actor-related events(add, remove, etc.), registered listeners
	// should be invoked synchronously
	//addListener(ActorEventListener)
	//removeListener(ActorEventListener)

	dispatcher() Dispatcher
	setDispatcher(Dispatcher)

	// create a top-level actor without supervisor
	ActorOf(builder ActorBuilder, name string) (ActorRef, error)
	// create a non top-level actor with given supervisor
	actorOf(builder ActorBuilder, name string, supervisorPath ActorPath) (ActorRef, error)

	Lookup(ActorPath) (ActorRef, error)

	// Remove(ActorPath) error
}

type DefaultActorSystem struct {
	TreeStore
	Dispatcher
}

func (t *DefaultActorSystem) ActorOf(builder ActorBuilder, name string) (ActorRef, error) {
	path := []string{name}
	unit, err := t.get(path)
	if unit != nil {
		return nil, fmt.Errorf("Actor %v already exists. Consider using Lookup(ActorPath) to get its reference", name)
	} else if err != nil {
		return nil, err
	}

	unit = &actorUnit{builder(), builder, name}
	err = t.put(unit, path)
	if err != nil {
		return nil, err
	}
	return t.createRef(unit, path), nil
}

// TODO more on this later
func (t *DefaultActorSystem) createRef(unit *actorUnit, path ActorPath) ActorRef {
	return &LocalActorRef{unit.actor, path}
}

// storage unit used by ActorSystem
type actorUnit struct {
	actor   Actor
	builder ActorBuilder
	name    string
}

// storage structure used by ActorSystem internally. no public APIs
type TreeStore interface {
	// return error if the given actor path already exists
	put(unit *actorUnit, path ActorPath) error

	// return error of the given actor path doesn't exist
	get(path ActorPath) (*actorUnit, error)

	// return error if the given actor path doesn't exist
	// otherwise return direct children only
	// if len(path) == 0, return all top-level units
	getChildren(path ActorPath) ([]*actorUnit, error)

	// return error if the given actor path doesn't exist
	getParent(path ActorPath) (*actorUnit, error)

	// if recursive == false, then return error if the actor has children;
	// otherwise, delete all children and the actor itself
	remove(path ActorPath, recursive bool) error
}

type TreeNode struct {
	unit     *actorUnit
	children map[string]*TreeNode
}

// in-memory tree
type DefaultTreeStore struct {
	size int
	root *TreeNode
}

// TODO to implement
type LocalActorRef struct {
	Actor
	actorPath ActorPath
}

func (t *LocalActorRef) path() ActorPath {
	return t.actorPath
}

// send message to the actor it represents
func (t *LocalActorRef) tell(msg Message) {

}

func (t *LocalActorRef) forward(msg Message, sender ActorRef) {

}

// a trie-like in memory store
func NewDefaultTreeStore() TreeStore {
	return new(DefaultTreeStore)
}

func (t *DefaultTreeStore) put(unit *actorUnit, path ActorPath) error {
	if _, err := t.get(path); err == nil {
		return fmt.Errorf("actor %v already exists", path)
	}

	var addHelper func(*TreeNode, *actorUnit, ActorPath) *TreeNode
	addHelper = func(root *TreeNode, unit *actorUnit, path ActorPath) *TreeNode {
		if root == nil {
			root = new(TreeNode)
		}
		if len(path) == 0 {
			root.unit = unit
		} else {
			if root.children == nil {
				root.children = make(map[string]*TreeNode)
			}

			child, exists := root.children[path[0]]
			if !exists {
				child = nil
			}
			root.children[path[0]] = addHelper(child, unit, path[1:])
		}
		return root
	}

	t.root = addHelper(t.root, unit, path)
	t.size += 1
	return nil
}

func (t *DefaultTreeStore) get(path ActorPath) (*actorUnit, error) {
	node := t.getNode(t.root, path)
	if node == nil {
		return nil, fmt.Errorf("actor %v doesn't exist", path)
	} else {
		return node.unit, nil
	}
}

// a helper function
func (t *DefaultTreeStore) getNode(root *TreeNode, path ActorPath) *TreeNode {
	if root == nil {
		return nil
	}
	if len(path) == 0 {
		return root
	}
	child, exists := root.children[path[0]]
	if !exists {
		child = nil
	}
	return t.getNode(child, path[1:])
}

// return all direct children
func (t *DefaultTreeStore) getChildren(path ActorPath) ([]*actorUnit, error) {
	node := t.getNode(t.root, path)
	if node == nil {
		return nil, fmt.Errorf("actor %v doesn't exist", path)
	} else {
		children := make([]*actorUnit, 0)
		for _, v := range node.children {
			if v.unit != nil {
				children = append(children, v.unit)
			}
		}
		return children, nil
	}
}

func (t *DefaultTreeStore) getParent(path ActorPath) (*actorUnit, error) {
	node := t.getNode(t.root, path)
	if node == nil {
		return nil, fmt.Errorf("actor %v doesn't exist", path)
	}

	parent := t.getNode(t.root, path[0:len(path)-1])
	if parent == nil {
		return nil, fmt.Errorf("parent %v doesn't exist", path[0:len(path)-1])
	} else {
		return parent.unit, nil
	}
}

func (t *DefaultTreeStore) remove(path ActorPath, recursive bool) error {
	node := t.getNode(t.root, path)
	if node == nil {
		return fmt.Errorf("actor %v doesn't exist", path)
	}

	if !recursive {
		if node.children != nil && len(node.children) > 0 {
			return fmt.Errorf("failed to delete actor %v because it has children", path)
		}
	}

	// delete children recursively
	if recursive {
		for k, _ := range node.children {
			err := t.remove(append(path, k), recursive)
			if err != nil {
				return err
			}
		}
	}

	// delete self from parent
	parent := t.getNode(t.root, path[0:len(path)-1])
	delete(parent.children, path[len(path)-1])
	if len(parent.children) == 0 {
		parent.children = nil
	}
	t.size -= 1

	return nil
}
