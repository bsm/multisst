package multisst

import "io"

const version = 1

var magic = []byte("MULTISST")

type shardOffset struct {
	Shard  uint32
	Offset int64
}

type shardInfo struct {
	Offset, Size int64
}

// --------------------------------------------------------------------

type readerWrapper struct{ r io.ReaderAt }

// ReadAt implements io.ReaderAt interface
func (r *readerWrapper) ReadAt(p []byte, off int64) (int, error) {
	return r.r.ReadAt(p, off)
}

// --------------------------------------------------------------------

type writerWrapper struct {
	w io.Writer

	offset int64
}

// Write implements io.Writer interface
func (w *writerWrapper) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.offset += int64(n)
	return n, err
}
