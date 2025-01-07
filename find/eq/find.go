package eq

import (
	"context"
	"iter"

	. "github.com/takanoriyanagitani/go-avro-find/util"
)

type Finder func(iter.Seq2[map[string]any, error]) IO[Result]

type MapToResult func(map[string]any) Result

func Find(
	maps iter.Seq2[map[string]any, error],
	map2result MapToResult,
) IO[Result] {
	return func(ctx context.Context) (Result, error) {
		for row, e := range maps {
			select {
			case <-ctx.Done():
				return ResultUnspecified, ctx.Err()
			default:
			}

			if nil != e {
				return ResultUnspecified, e
			}

			var res Result = map2result(row)
			if ResultFound == res {
				return ResultFound, nil
			}
		}

		return ResultNotFound, nil
	}
}

func (m MapToResult) Find(maps iter.Seq2[map[string]any, error]) IO[Result] {
	return Find(maps, m)
}
