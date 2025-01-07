package eq

//go:generate stringer -type=Result
type Result uint8

const (
	ResultUnspecified Result = iota
	ResultFound       Result = iota
	ResultNotFound    Result = iota
)
