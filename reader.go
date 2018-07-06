package multisst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	ErrNotExist = errors.New("multisst: shard does not exist")

	errBadHeader  = errors.New("multisst: bad format, invalid header")
	errBadVersion = errors.New("multisst: bad format, unknown version")
)

// Reader instances can read multiple SST shards from a single reader, e.g. a File.
type Reader struct {
	r io.ReaderAt

	shards  map[uint32]*table.Reader
	offsets map[uint32]int64
	sizes   map[uint32]int64
	mu      sync.RWMutex
}

// NewReader requires an instance of the underlying reader
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	sb := make([]byte, 12)

	// read header
	if _, err := r.ReadAt(sb[:9], 0); err != nil {
		return nil, err
	}
	if !bytes.Equal(sb[:8], magic) {
		return nil, errBadHeader
	}
	if sb[8] != version {
		return nil, errBadVersion
	}

	// read footer
	if _, err := r.ReadAt(sb[:8], size-8); err != nil {
		return nil, err
	}
	offsets := make(map[uint32]int64)
	sizes := make(map[uint32]int64)
	last := shardOffset{}
	for pos := int64(binary.LittleEndian.Uint64(sb[:8])); pos < size-8; pos += 12 {
		if _, err := r.ReadAt(sb, pos); err != nil {
			return nil, err
		}

		shard := binary.LittleEndian.Uint32(sb[:4])
		offset := int64(binary.LittleEndian.Uint64(sb[4:]))
		offsets[shard] = offset
		if last.Offset > 0 {
			sizes[last.Shard] = offset - last.Offset
		}
		last.Shard, last.Offset = shard, offset
	}
	if last.Offset > 0 {
		sizes[last.Shard] = size - 8 - last.Offset
	}

	return &Reader{
		r:       r,
		shards:  make(map[uint32]*table.Reader),
		offsets: offsets,
		sizes:   sizes,
	}, nil
}

// Get returns a shard reader or ErrNotExist if a shard does not exist.
func (r *Reader) Get(shard uint32, opt *opt.Options) (ShardReader, error) {
	r.mu.RLock()
	reader, rok := r.shards[shard]
	offset, ook := r.offsets[shard]
	r.mu.RUnlock()

	if rok {
		return reader, nil
	}
	if !ook {
		return nil, ErrNotExist
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reader, ok := r.shards[shard]; ok {
		return reader, nil
	}

	offset, size := r.offsets[shard], r.sizes[shard]
	section := io.NewSectionReader(r.r, offset, size)
	fileDesc := storage.FileDesc{Type: storage.TypeTable, Num: int64(shard)}

	reader, err := table.NewReader(section, section.Size(), fileDesc, nil, nil, opt)
	if err != nil {
		return nil, err
	}

	r.shards[shard] = reader
	return reader, nil
}

// Close closes the reader and all handles.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for shard, tr := range r.shards {
		tr.Release()
		delete(r.shards, shard)
	}
	return nil
}

// ShardReader is able to read contents of a shard.
type ShardReader interface {
	// NewIterator creates an iterator from the table.
	//
	// Slice allows slicing the iterator to only contains keys in the given
	// range. A nil Range.Start is treated as a key before all keys in the
	// table. And a nil Range.Limit is treated as a key after all keys in
	// the table.
	//
	// The returned iterator is not safe for concurrent use and should be released
	// after use.
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator

	// Find finds key/value pair whose key is greater than or equal to the
	// given key. It returns ErrNotFound if the table doesn't contain
	// such pair.
	// If filtered is true then the nearest 'block' will be checked against
	// 'filter data' (if present) and will immediately return ErrNotFound if
	// 'filter data' indicates that such pair doesn't exist.
	//
	// The caller may modify the contents of the returned slice as it is its
	// own copy.
	// It is safe to modify the contents of the argument after Find returns.
	Find(key []byte, filtered bool, ro *opt.ReadOptions) (rkey, value []byte, err error)

	// FindKey finds key that is greater than or equal to the given key.
	// It returns ErrNotFound if the table doesn't contain such key.
	// If filtered is true then the nearest 'block' will be checked against
	// 'filter data' (if present) and will immediately return ErrNotFound if
	// 'filter data' indicates that such key doesn't exist.
	//
	// The caller may modify the contents of the returned slice as it is its
	// own copy.
	// It is safe to modify the contents of the argument after Find returns.
	FindKey(key []byte, filtered bool, ro *opt.ReadOptions) (rkey []byte, err error)

	// Get gets the value for the given key. It returns errors.ErrNotFound
	// if the table does not contain the key.
	//
	// The caller may modify the contents of the returned slice as it is its
	// own copy.
	// It is safe to modify the contents of the argument after Find returns.
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
}
