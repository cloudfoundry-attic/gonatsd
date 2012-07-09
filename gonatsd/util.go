// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"strconv"
	"strings"
	"unicode"
)

func parseInt(value string) (int, error) {
	parsed, err := strconv.ParseInt(value, 10, 0)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}

func fieldsN(s string, f func(rune) bool, n int) []string {
	result := make([]string, 0, n)

	start := -1
	current := 0
	for index, rune := range s {
		if f(rune) {
			if start >= 0 {
				result = append(result, s[start:index])
				current++
				start = -1
			}
		} else if start == -1 {
			start = index
			if current+1 == n {
				break
			}
		}
	}

	if start >= 0 {
		result = append(result, s[start:])
	}

	return result
}

func isNotSpace(r rune) bool {
	return !unicode.IsSpace(r)
}

func IsAllSpace(s string) bool {
	return strings.IndexFunc(s, isNotSpace) == -1
}

func ensureValidPublishedSubject(subject string) bool {
	parts := strings.Split(subject, ".")
	for _, part := range parts {
		if len(part) == 0 {
			return false
		}

		match := strings.IndexAny(part, "*>")
		if match != -1 {
			return false
		}
	}
	return true
}

func ensureValidSubscribedSubject(subject string) bool {
	parts := strings.Split(subject, ".")

	for index, part := range parts {
		if len(part) == 0 {
			return false
		}

		match := strings.IndexAny(part, "*>")
		switch match {
		case -1:
		case 0:
			if len(part) != 1 {
				return false
			}
			if part[match] == '>' && index != len(parts)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
