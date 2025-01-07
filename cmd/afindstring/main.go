package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"

	. "github.com/takanoriyanagitani/go-avro-find/util"

	eq "github.com/takanoriyanagitani/go-avro-find/find/eq"
	es "github.com/takanoriyanagitani/go-avro-find/find/string"
	se "github.com/takanoriyanagitani/go-avro-find/find/string/eq"

	dh "github.com/takanoriyanagitani/go-avro-find/avro/dec/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var targetColName IO[string] = EnvValByKey("ENV_TARGET_COL_NAME")
var targetValue IO[string] = EnvValByKey("ENV_TARGET_COL_VALUE")

var map2result IO[eq.MapToResult] = Bind(
	All(
		targetColName,
		targetValue,
	),
	Lift(func(s []string) (eq.MapToResult, error) {
		return se.
			EqString(s[1]).
			ToMapToResult(es.TargetColumnName(s[0])), nil
	}),
)

func stdin2filenames(_ context.Context) (iter.Seq[string], error) {
	return func(yield func(string) bool) {
		var rdr io.Reader = os.Stdin
		var s *bufio.Scanner = bufio.NewScanner(rdr)
		for s.Scan() {
			if !yield(s.Text()) {
				return
			}
		}
	}, nil
}

var stdin2names IO[iter.Seq[string]] = stdin2filenames

func name2stdout(name string) func(eq.Result) IO[Void] {
	return func(r eq.Result) IO[Void] {
		return func(_ context.Context) (Void, error) {
			if eq.ResultFound == r {
				fmt.Println(name)
			}
			return Empty, nil
		}
	}
}

func names2results2founds2stdout(
	names iter.Seq[string],
	map2res eq.MapToResult,
) IO[Void] {
	return func(ctx context.Context) (Void, error) {
		for name := range names {
			var rows iter.Seq2[map[string]any, error] = dh.
				FilenameToMapsDefault(name)
			var ires IO[eq.Result] = map2res.Find(rows)
			var res2stdout func(eq.Result) IO[Void] = name2stdout(name)
			_, e := Bind(ires, res2stdout)(ctx)
			if nil != e {
				return Empty, e
			}
		}

		return Empty, nil
	}
}

var stdin2names2results2founds2stdout IO[Void] = Bind(
	map2result,
	func(m2r eq.MapToResult) IO[Void] {
		return Bind(
			stdin2names,
			func(names iter.Seq[string]) IO[Void] {
				return names2results2founds2stdout(names, m2r)
			},
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2names2results2founds2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
