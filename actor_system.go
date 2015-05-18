// actor_system.go
package akka

import (
	"fmt"
)

type ActorSystem interface {
	// TODO should keep all methods that directly interact with actors private
	TreeStore

	// for actor-related events(add, remove, etc.), registered listeners
	// should be invoked synchronously
	addListener(listener ActorEventListener)
	removeListener(listener ActorEventListener)

	bind(dispatcher Dispatcher)
	getDispatcher() Dispatcher
}

type TreeStore interface {
	// return error if the given actor path already exists
	put(actor Actor, path ActorPath) (ActorRef, error)

	// return error of the given actor path doesn't exist
	get(path ActorPath) (ActorRef, error)

	// return error if the given actor path doesn't exist
	getChildren(path ActorPath) ([]ActorRef, error)

	// return error if the given actor path doesn't exist
	getParent(path ActorPath) (ActorRef, error)

	// if recursive == false, then return error if the actor has children;
	// otherwise, delete all children and the actor itself
	remove(path ActorPath, recursive bool) error
}

type TreeNode struct {
	actor    Actor
	children map[string]*TreeNode
}

// in-memory tree
type DefaultTreeStore struct {
	size int
	root *TreeNode
}

type LocalActorRef struct {
	Actor
	ActorPath
}

func (t *LocalActorRef) path() ActorPath {
	return nil
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

func (t *DefaultTreeStore) put(actor Actor, path ActorPath) (ActorRef, error) {
	if _, err := t.get(path); err == nil {
		return nil, fmt.Errorf("actor %v already exists", path)
	}

	var addHelper func(*TreeNode, Actor, ActorPath) *TreeNode
	addHelper = func(root *TreeNode, actor Actor, path ActorPath) *TreeNode {
		if root == nil {
			root = new(TreeNode)
		}
		if len(path) == 0 {
			root.actor = actor
		} else {
			if root.children == nil {
				root.children = make(map[string]*TreeNode)
			}

			child, exists := root.children[path[0]]
			if !exists {
				child = nil
			}
			root.children[path[0]] = addHelper(child, actor, path[1:])
		}
		return root
	}

	t.root = addHelper(t.root, actor, path)
	t.size += 1
	return &LocalActorRef{actor, path}, nil
}

func (t *DefaultTreeStore) get(path ActorPath) (ActorRef, error) {
	var getHelper func(*TreeNode, ActorPath) *TreeNode
	getHelper = func(root *TreeNode, path ActorPath) *TreeNode {
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
		return getHelper(child, path[1:])
	}

	node := getHelper(t.root, path)
	if node == nil {
		return nil, fmt.Errorf("actor %v doesn't exist", path)
	} else {
		return &LocalActorRef{node.actor, path}, nil
	}
}

func (t *DefaultTreeStore) getChildren(path ActorPath) ([]ActorRef, error) {
	return nil, nil
}

func (t *DefaultTreeStore) getParent(path ActorPath) (ActorRef, error) {
	return nil, nil
}

func (t *DefaultTreeStore) remove(path ActorPath, recursive bool) error {
	return nil
}
