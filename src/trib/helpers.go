package trib

import "encoding/json"

/**
 * TribList: used for sorting list of tribs
 */
type TribList []*Trib

func (self TribList) Len() int {
	return len(self)
}

func (self TribList) Less(i, j int) bool {
	if self[i].Clock < self[j].Clock {
		return true
	} else if self[j].Clock < self[i].Clock {
		return false
	}

	if self[i].Time.Before(self[j].Time) {
		return true
	} else if self[j].Time.Before(self[i].Time) {
		return false
	}

	if self[i].User < self[j].User {
		return true
	} else if self[j].User < self[i].User {
		return false
	}

	return self[i].Message < self[j].Message
}

func (self TribList) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

/**
 * Converts a trib.Trib into a string
 */
func TribToString(t *Trib) (string, error) {
	b, e := json.Marshal(t)
	return string(b), e
}


/**
 * Converts a string into a trib.Trib
 */
func StringToTrib(s string) (*Trib, error) {
	t := new(Trib)
	e := json.Unmarshal([]byte(s), t)

	return t, e
}

