package bin_back

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/utils"
	"encoding/json"
	"sort"
)

type Log struct {
	Key string
	Operation string
	Value string
	Clock uint64
}

func CreateLog(key string, operation string, value string, clock uint64) *Log {
	log := new(Log)
	log.Key = key
	log.Operation = operation
	log.Value = value
	log.Clock = clock

	return log
}

/**
 * ListLogList: used for sorting
 */
type ListLogList []*Log
func (self ListLogList) Len() int {
	return len(self)
}

func (self ListLogList) Less(i, j int) bool {
	if self[i].Clock != self[j].Clock{
		return self[i].Clock < self[j].Clock
	}

	if self[i].Operation != self[j].Operation{
		return self[i].Operation < self[j].Operation
	}

	return self[i].Value < self[j].Value
}

func (self ListLogList) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

/**
 * Converts a json string into Log object
 */
func StringToLog(s string) (*Log, error) {
	log := new(Log)
	e := json.Unmarshal([]byte(s), log)

	return log, e
}

func ParseLog(keyValueLogList []string) ([]*Log, error) {
	ret := make([]*Log, 0)
	for _, s := range keyValueLogList {
		log, e := StringToLog(s)
		if e != nil {
			return nil, e
		}

		ret = append(ret, log)
	}

	return ret, nil
}

func ParseListLog(logStringList []string) ([]*Log, error) {
	logList, e := ParseLog(utils.Unique(logStringList))
	if e != nil {
		return nil, e
	}

	sort.Sort(ListLogList(logList))

	return logList, nil
}

func ReplayKeyValueLog(keyValueLogList []*Log) *Log {
	ret := &Log{Value: "", Clock: uint64(0)}

	for _, log := range keyValueLogList {
		if log.Clock == ret.Clock {
			if ret.Value < log.Value {
				ret = log
			}
		} else if log.Clock > ret.Clock {
			ret = log
		}
	}

	return ret
}

func ReplayListLog(listLogList []*Log) ([]string) {
	result := make([]string, 0)
	ignore := make(map[string]bool)

	for i := len(listLogList) - 1; i >= 0; i-- {
		log := listLogList[i]

		ig, ok := ignore[log.Value]
		if !ok {
			ignore[log.Value] = false
		} else if ig {
			continue
		}

		if log.Operation == bin_config.ListLogDelete {
			ignore[log.Value] = true
		} else {
			result = append(result, log.Value)
		}
	}

	res := make([]string, 0, len(result))
	for i := len(result) - 1; i >= 0; i-- {
		res = append(res, result[i])
	}

	return res
}
