package utils

import (
	"encoding/json"
	"hash/fnv"
)

/* Helper functions from here */

/**
 * Use Fnv to convert a string into an integer
 */
func StringToFnvNumber(s string) int {
	h := fnv.New128().Sum([]byte(s))

	sum := 0
	for _, v := range h {
		sum += int(v)
	}

	return sum
}

/**
 * Deduplicates a string slice
 */
func Unique(s []string) []string {
	keys := make(map[string]bool)
	var list []string

	for _, v := range s {
		if _, value := keys[v]; !value {
			keys[v] = true
			list = append(list, v)
		}
	}

	return list
}

/**
 * Converts an object into a json string
 */
func ObjectToString(o interface{}) (string, error) {
	b, e := json.Marshal(o)
	return string(b), e
}

func IndexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}
