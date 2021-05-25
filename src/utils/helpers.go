package utils

import (
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
