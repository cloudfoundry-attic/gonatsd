// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"strings"
)

// Trie - prefix tree.
type Trie struct {
	root   *trieNode
	sep    string
	nodes  int
	values int
}

type trieNode struct {
	Name     string
	Children map[string]*trieNode
	values   []interface{}
}

func NewTrie(sep string) *Trie {
	trie := &Trie{}
	trie.root = &trieNode{}
	trie.sep = sep
	return trie
}

func (t *Trie) Insert(key string, value interface{}) {
	parts := strings.Split(key, t.sep)

	node := t.root
	for _, part := range parts {
		if node.Children == nil {
			node.Children = make(map[string]*trieNode)
		}
		child := node.Children[part]
		if child == nil {
			child = &trieNode{Name: part}
			node.Children[part] = child
			t.nodes++
		}
		node = child
	}

	if node.values == nil {
		node.values = make([]interface{}, 0, 1)
	}
	node.values = append(node.values, value)
	t.values++
}

func (t *Trie) Delete(key string, value interface{}) bool {
	parts := strings.Split(key, t.sep)

	// Need the nodes for pruning later
	nodes := make([]*trieNode, len(parts))

	node := t.root
	for i, part := range parts {
		if node.Children == nil {
			return false
		}
		child := node.Children[part]
		if child == nil {
			return false
		}
		node = child
		nodes[i] = node
	}

	if node.values == nil {
		return false
	}

	for i, v := range node.values {
		if v == value {
			lastIndex := len(node.values) - 1
			node.values[i] = node.values[lastIndex]
			node.values = node.values[:lastIndex]
			t.values--
			if len(node.values) == 0 && len(node.Children) == 0 {
				t.pruneNodes(nodes)
			}
			return true
		}
	}
	return false
}

func (t *Trie) Nodes() int {
	return t.nodes
}

func (t *Trie) Values() int {
	return t.values
}

var emptyNodeSlice = make([]*trieNode, 0, 0)

type Matcher func(*trieNode, string) ([]*trieNode, []*trieNode)

var BasicMatcher = func(node *trieNode, token string) ([]*trieNode, []*trieNode) {
	match := node.Children[token]
	if match != nil {
		return []*trieNode{match}, nil
	}
	return emptyNodeSlice, nil
}

var WildcardMatcher = func(node *trieNode, token string) ([]*trieNode, []*trieNode) {
	matches := make([]*trieNode, 0, 3)

	match := node.Children[token]
	if match != nil {
		matches = append(matches, match)
	}

	match = node.Children["*"]
	if match != nil {
		matches = append(matches, match)
	}

	match = node.Children[">"]
	if match != nil {
		return matches, []*trieNode{match}
	}

	return matches, nil
}

func (t *Trie) Match(key string, matcher Matcher) []interface{} {
	values := make([]interface{}, 0, 1)
	parts := strings.Split(key, t.sep)

	currentLevel := []*trieNode{t.root}
	for _, part := range parts {
		nextLevel := make([]*trieNode, 0, 1)
		for _, node := range currentLevel {
			matches, valueNodes := matcher(node, part)
			nextLevel = append(nextLevel, matches...)
			if valueNodes != nil {
				for _, valueNode := range valueNodes {
					values = append(values, valueNode.values...)
				}
			}
		}
		if len(nextLevel) == 0 {
			return values
		}
		currentLevel = nextLevel
	}
	for _, node := range currentLevel {
		values = append(values, node.values...)
	}
	return values
}

func (t *Trie) pruneNodes(nodes []*trieNode) {
	length := len(nodes)
	var last *trieNode = nil
	for i := length - 1; i >= 0; i-- {
		node := nodes[i]
		if last != nil {
			delete(node.Children, last.Name)
			t.nodes--
		}
		if len(node.values) == 0 && len(node.Children) == 0 {
			node.values = nil
			node.Children = nil
		} else {
			return
		}
		last = node
	}

	delete(t.root.Children, last.Name)
	t.nodes--
}
