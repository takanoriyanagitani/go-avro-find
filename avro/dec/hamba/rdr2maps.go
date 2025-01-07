package dec

import (
	"bufio"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	ga "github.com/takanoriyanagitani/go-avro-find"
	. "github.com/takanoriyanagitani/go-avro-find/util"
)

func ReaderToMapsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}
		var br io.Reader = bufio.NewReader(rdr)

		dec, e := ho.NewDecoder(br, opts...)
		if nil != e {
			yield(buf, e)
			return
		}

		for dec.HasNext() {
			clear(buf)

			e = dec.Decode(&buf)
			if !yield(buf, e) {
				return
			}
		}
	}
}

func ConfigToOpts(cfg ga.DecodeConfig) []ho.DecoderFunc {
	var blobSizeMax int = cfg.BlobSizeMax

	var hcfg ha.Config
	hcfg.MaxByteSliceSize = blobSizeMax
	var hapi ha.API = hcfg.Freeze()

	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMaps(
	rdr io.Reader,
	cfg ga.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = ConfigToOpts(cfg)
	return ReaderToMapsHamba(
		rdr,
		opts...,
	)
}

func FileLikeToMaps(
	f io.ReadCloser,
	cfg ga.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		defer f.Close()
		var i iter.Seq2[map[string]any, error] = ReaderToMaps(f, cfg)
		for row, e := range i {
			if !yield(row, e) {
				return
			}
		}
	}
}

func FilenameToMaps(
	filename string,
	cfg ga.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	f, e := os.Open(filename)
	if nil != e {
		return func(yield func(map[string]any, error) bool) {
			yield(map[string]any{}, e)
		}
	}
	return FileLikeToMaps(f, cfg)
}

func FilenameToMapsDefault(
	filename string,
) iter.Seq2[map[string]any, error] {
	return FilenameToMaps(filename, ga.DecodeConfigDefault)
}

func FilenamesToMaps(
	filenames iter.Seq[string],
	cfg ga.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		for filename := range filenames {
			var i iter.Seq2[map[string]any, error] = FilenameToMaps(
				filename,
				cfg,
			)
			for row, e := range i {
				if !yield(row, e) {
					return
				}
			}
		}
	}
}

func FilenamesToMapsDefault(
	filenames iter.Seq[string],
) iter.Seq2[map[string]any, error] {
	return FilenamesToMaps(filenames, ga.DecodeConfigDefault)
}

func StdinToMaps(
	cfg ga.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin, cfg)
}

var StdinToMapsDefault IO[iter.Seq2[map[string]any, error]] = OfFn(
	func() iter.Seq2[map[string]any, error] {
		return StdinToMaps(ga.DecodeConfigDefault)
	},
)
