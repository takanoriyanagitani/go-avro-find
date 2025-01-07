package findstr

import (
	eq "github.com/takanoriyanagitani/go-avro-find/find/eq"
)

type StringToResult func(string) eq.Result
type MapToString func(map[string]any) (string, bool)
type TargetColumnName string

func (m MapToString) ToMapToResult(s2r StringToResult) eq.MapToResult {
	return func(i map[string]any) eq.Result {
		str, found := m(i)
		if !found {
			return eq.ResultUnspecified
		}

		return s2r(str)
	}
}

func (t TargetColumnName) ToMapToString() MapToString {
	return func(i map[string]any) (string, bool) {
		var a any = i[string(t)]
		switch s := a.(type) {
		case string:
			return s, true
		default:
			return "", false
		}
	}
}

func (t TargetColumnName) ToMapToResult(s2r StringToResult) eq.MapToResult {
	return t.ToMapToString().ToMapToResult(s2r)
}
