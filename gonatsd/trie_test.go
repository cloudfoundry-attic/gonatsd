// Copyright (c) 2012 VMware, Inc.

package gonatsd_test

import (
	. "gonatsd/gonatsd"
	. "launchpad.net/gocheck"
	"math/rand"
	"strings"
)

type TrieSuite struct{}

var _ = Suite(&TrieSuite{})

func (s *TrieSuite) TestTrieInsert(c *C) {
	trie := NewTrie(".")
	matches := trie.Match("foo.bar", BasicMatcher)
	c.Check(matches, HasLen, 0)
	c.Check(trie.Nodes(), Equals, 0)
	c.Check(trie.Values(), Equals, 0)

	trie.Insert("foo.bar", 42)
	matches = trie.Match("foo.bar", BasicMatcher)
	c.Check(matches, HasLen, 1)
	c.Check(trie.Nodes(), Equals, 2)
	c.Check(trie.Values(), Equals, 1)
}

func (s *TrieSuite) TestTrieDelete(c *C) {
	trie := NewTrie(".")
	trie.Insert("foo.bar", "42")
	result := trie.Delete("foo.bar", "42")
	c.Check(result, Equals, true)
	result = trie.Delete("foo.bar", "42")
	c.Check(result, Equals, false)
	c.Check(trie.Nodes(), Equals, 0)
}

func (s *TrieSuite) TestTrieMatch(c *C) {
	trie := NewTrie(".")
	trie.Insert("foo", "2")
	trie.Insert("foo.bar", "3")
	trie.Insert("hello.world", "1")

	matches := trie.Match("foo", BasicMatcher)
	c.Check(matches, HasLen, 1)

	matches = trie.Match("foo.bar", BasicMatcher)
	c.Check(matches, HasLen, 1)

	matches = trie.Match("hello.world", BasicMatcher)
	c.Check(matches, HasLen, 1)

	matches = trie.Match("baz", BasicMatcher)
	c.Check(matches, HasLen, 0)
}

func (s *TrieSuite) TestWildcardTrieMatch(c *C) {
	trie := NewTrie(".")
	trie.Insert("foo", "1")
	trie.Insert("foo.bar", "2")
	trie.Insert("hello.*", "3")
	trie.Insert("*.*", "4")
	trie.Insert("foo.>", "5")
	trie.Insert("*.>", "6")
	trie.Insert(">", "7")

	matches := trie.Match("foo", WildcardMatcher)
	c.Check(matches, HasLen, 2)

	matches = trie.Match("foo.bar", WildcardMatcher)
	c.Check(matches, HasLen, 5)

	matches = trie.Match("hello.world", WildcardMatcher)
	c.Check(matches, HasLen, 4)

	matches = trie.Match("baz", WildcardMatcher)
	c.Check(matches, HasLen, 1)
}

const VALID_CHARS = "abcdefghijklmnopqrstuvwxyz"

func (s *TrieSuite) BenchmarkTrieInsert(c *C) {
	words := make([]string, 100)
	for i := 0; i < 100; i++ {
		word := make([]byte, 0, rand.Intn(32))
		for j := range word {
			word[j] = VALID_CHARS[rand.Intn(len(VALID_CHARS))]
		}

		words[i] = string(word)
	}

	c.ResetTimer()
	c.StartTimer()
	trie := NewTrie(".")
	for i := 0; i < c.N; i++ {
		numTokens := rand.Intn(10)
		tokens := make([]string, numTokens)
		for j := range tokens {
			tokens[j] = words[rand.Intn(len(words))]
		}
		trie.Insert(strings.Join(tokens, "."), rand.Intn(10))
	}
}

var tokens = [...]string{"a", "bb", "ccc", "dddd", "eeeee", "ffffff", "ggggggg", "hhhhhhhh",
	"iiiiiiiii", "jjjjjjjjjj"}

func createMatchTrie() *Trie {
	trie := NewTrie(".")
	createMatchTrieHelper(trie, "", 0)
	return trie
}

func createMatchTrieHelper(trie *Trie, prefix string, depth int) {
	if depth > 4 {
		return
	}

	for _, token := range tokens {
		if prefix == "" {
			prefix = token
		} else {
			prefix = prefix + "." + token
		}
		trie.Insert(prefix, prefix)
		createMatchTrieHelper(trie, prefix, depth+1)
	}
}

func (s *TrieSuite) BenchmarkTrieWildcardMatch(c *C) {
	trie := createMatchTrie()
	c.ResetTimer()
	c.StartTimer()
	for i := 0; i < c.N; i++ {
		trie.Match("dddd.a.eeeee.bb", WildcardMatcher)
		trie.Match("a.bb.cccc.dddd", WildcardMatcher)
		trie.Match("jjjjjjjjjj.iiiiiiiii.hhhhhhhh", WildcardMatcher)
	}
}

func (s *TrieSuite) BenchmarkTrieBasicMatch(c *C) {
	trie := createMatchTrie()
	c.ResetTimer()
	c.StartTimer()
	for i := 0; i < c.N; i++ {
		trie.Match("dddd.a.eeeee.bb", BasicMatcher)
		trie.Match("a.bb.cccc.dddd", BasicMatcher)
		trie.Match("jjjjjjjjjj.iiiiiiiii.hhhhhhhh", BasicMatcher)
	}
}
