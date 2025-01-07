package findstr

import (
	eq "github.com/takanoriyanagitani/go-avro-find/find/eq"
	es "github.com/takanoriyanagitani/go-avro-find/find/string"
)

type EqString string

func (e EqString) ToStringToResult() es.StringToResult {
	return func(s string) eq.Result {
		switch s == string(e) {
		case true:
			return eq.ResultFound
		default:
			return eq.ResultNotFound
		}
	}
}

func (e EqString) ToMapToResult(t es.TargetColumnName) eq.MapToResult {
	return t.ToMapToResult(e.ToStringToResult())
}
