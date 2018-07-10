package multisst

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/table"
)

var (
	errShardOpen    = errors.New("multisst: another shard is aready open")
	errShardExists  = errors.New("multisst: shard aready exists")
	errWriterClosed = errors.New("multisst: writer is closed")
)

// Writer instances can wrap multiple shards into single writer.
type Writer struct {
	w writerWrapper

	offsets []shardOffset
	current *ShardWriter

	closed bool
	mu     sync.Mutex
}

// NewWriter creates a writer on top of an existing writer.
func NewWriter(w io.Writer) (*Writer, error) {
	ww := writerWrapper{w: w}

	if _, err := ww.Write(magic); err != nil {
		return nil, err
	}
	if _, err := ww.Write([]byte{version}); err != nil {
		return nil, err
	}
	return &Writer{w: ww}, nil
}

// Create creates a new shard and returns a ShardWriter. A writer can only handle one shard at a time,
// please don't forget to call ShardWriter.Close() before trying to create the next shard.
func (w *Writer) Create(shard uint64, opt *opt.Options) (*ShardWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, errWriterClosed
	}
	if w.current != nil {
		return nil, errShardOpen
	}

	for _, so := range w.offsets {
		if so.Shard == shard {
			return nil, errShardExists
		}
	}

	w.current = &ShardWriter{
		Writer: table.NewWriter(&w.w, opt),
		parent: w,
	}
	w.offsets = append(w.offsets, shardOffset{Shard: shard, Offset: w.w.offset})
	return w.current, nil
}

// Close will close any remaining shards and flush the index.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	var err error
	if w.current != nil {
		err = w.current.Close()
	}
	if err == nil {
		err = w.flush()
	}

	w.closed = true
	w.offsets = nil
	return err
}

func (w *Writer) flush() error {
	pos := w.w.offset
	sb := make([]byte, 16)
	for _, so := range w.offsets {
		binary.LittleEndian.PutUint64(sb[:8], so.Shard)
		binary.LittleEndian.PutUint64(sb[8:], uint64(so.Offset))
		if _, err := w.w.Write(sb); err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint64(sb[:8], uint64(pos))
	if _, err := w.w.Write(sb[:8]); err != nil {
		return err
	}
	return nil
}

// --------------------------------------------------------------------

type ShardWriter struct {
	*table.Writer
	parent *Writer
}

// Close closes the writer and releases the resource.
func (w *ShardWriter) Close() error {
	err := w.Writer.Close()
	w.parent.current = nil
	return err
}
