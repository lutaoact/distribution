// +build noresumabledigest

package storage

import (
	"github.com/kirk-enterprise/distribution/context"
)

// resumeHashAt is a noop when resumable digest support is disabled.
func (bw *blobWriter) resumeDigest(ctx context.Context) error {
	return errResumableDigestNotAvailable
}

// storeHashState is a noop when resumable digest support is disabled.
func (bw *blobWriter) storeHashState(ctx context.Context) error {
	return errResumableDigestNotAvailable
}
