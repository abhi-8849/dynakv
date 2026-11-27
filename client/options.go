package client

// ReadOpt customizes one Get.
type ReadOpt func(*readOpts)

// WriteOpt customizes one Put or Delete.
type WriteOpt func(*writeOpts)

type readOpts struct{ r int }

type writeOpts struct {
	w    int
	vctx []byte
}

// WithR sets how many replicas must reply to a Get; 0 uses the node default.
func WithR(n int) ReadOpt { return func(o *readOpts) { o.r = n } }

// WithW sets how many replicas must ack a write; 0 uses the node default.
func WithW(n int) WriteOpt { return func(o *writeOpts) { o.w = n } }

// WithContext passes the context from a previous Get so the write supersedes the versions that Get returned.
func WithContext(vctx []byte) WriteOpt { return func(o *writeOpts) { o.vctx = vctx } }
