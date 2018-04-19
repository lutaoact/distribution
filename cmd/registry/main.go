package main

import (
	_ "net/http/pprof"

	"github.com/kirk-enterprise/distribution/registry"
	_ "github.com/kirk-enterprise/distribution/registry/auth/htpasswd"
	_ "github.com/kirk-enterprise/distribution/registry/auth/silly"
	_ "github.com/kirk-enterprise/distribution/registry/auth/token"
	_ "github.com/kirk-enterprise/distribution/registry/proxy"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/azure"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/filesystem"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/gcs"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/inmemory"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/kodo"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/middleware/cloudfront"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/middleware/redirect"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/oss"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/s3-aws"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/s3-goamz"
	_ "github.com/kirk-enterprise/distribution/registry/storage/driver/swift"
)

func main() {
	registry.RootCmd.Execute()
}
