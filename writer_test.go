package multisst

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Writer", func() {
	var buf bytes.Buffer
	var subject *Writer

	BeforeEach(func() {
		buf.Reset()

		w, err := NewWriter(&buf)
		Expect(err).ToNot(HaveOccurred())
		subject = w
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("should init/close writers", func() {
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(Equal(17))
		Expect(buf.String()).To(Equal("MULTISST\x01\x09\x00\x00\x00\x00\x00\x00\x00"))
	})

	It("should write shards", func() {
		sw, err := subject.Create(27, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(seedShard(sw, 4)).To(Succeed())
		Expect(sw.Close()).To(Succeed())

		sw, err = subject.Create(12, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(seedShard(sw, 4)).To(Succeed())
		Expect(sw.Close()).To(Succeed())

		sw, err = subject.Create(55, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(seedShard(sw, 4)).To(Succeed())
		Expect(sw.Close()).To(Succeed())

		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(Equal(431))
	})

	It("should only allow a shard at a time", func() {
		sw, err := subject.Create(27, nil)
		Expect(err).ToNot(HaveOccurred())
		defer sw.Close()

		_, err = subject.Create(28, nil)
		Expect(err).To(MatchError(errShardOpen))
	})

	It("should not allow duplicate shards", func() {
		sw, err := subject.Create(27, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(sw.Close()).To(Succeed())

		_, err = subject.Create(27, nil)
		Expect(err).To(MatchError(errShardExists))
	})

})
