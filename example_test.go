package multisst_test

import (
	"io/ioutil"

	"github.com/bsm/multisst"
)

func ExampleWriter() {
	f, err := ioutil.TempFile("", "multisst-example")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Open a writer
	w, err := multisst.NewWriter(f)
	if err != nil {
		panic(err)
	}

	// Write shard #0
	sw0, err := w.Create(0, nil)
	if err != nil {
		panic(err)
	}
	_ = sw0.Append([]byte("key1"), []byte("val"))
	_ = sw0.Append([]byte("key2"), []byte("val"))
	_ = sw0.Append([]byte("key3"), []byte("val"))
	if err := sw0.Close(); err != nil {
		panic(err)
	}

	// Write shard #1
	sw1, err := w.Create(0, nil)
	if err != nil {
		panic(err)
	}
	_ = sw1.Append([]byte("key2"), []byte("val"))
	_ = sw1.Append([]byte("key4"), []byte("val"))
	_ = sw1.Append([]byte("key6"), []byte("val"))
	if err := sw1.Close(); err != nil {
		panic(err)
	}

	if err := w.Close(); err != nil {
		panic(err)
	}
}
