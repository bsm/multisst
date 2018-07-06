package multisst

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader", func() {
	var subject *Reader

	BeforeEach(func() {
		buf := new(bytes.Buffer)
		w, err := NewWriter(buf)
		Expect(err).ToNot(HaveOccurred())

		for _, x := range []struct {
			S uint32
			N int
		}{
			{S: 27, N: 12},
			{S: 14, N: 8},
			{S: 55, N: 22},
			{S: 89, N: 17},
		} {
			sw, err := w.Create(x.S, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(seedShard(sw, x.N)).To(Succeed())
			Expect(sw.Close()).To(Succeed())
		}
		Expect(w.Close()).To(Succeed())

		subject, err = NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("should init", func() {
		Expect(subject.offsets).To(Equal(map[uint32]int64{27: 9, 14: 166, 55: 308, 89: 482}))
		Expect(subject.sizes).To(Equal(map[uint32]int64{14: 142, 27: 157, 55: 174, 89: 217}))
	})

	It("should get readers", func() {
		sr, err := subject.Get(27, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(sr.Get([]byte("KEY001"), nil)).To(Equal([]byte("VAL")))

		_, err = subject.Get(28, nil)
		Expect(err).To(MatchError(ErrNotExist))
	})

})
