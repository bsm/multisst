package multisst

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "multisst")
}

func seedShard(w *ShardWriter, n int) error {
	val := []byte("VAL")
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("KEY%03d", i)
		if err := w.Append([]byte(key), val); err != nil {
			return err
		}
	}
	return nil
}
