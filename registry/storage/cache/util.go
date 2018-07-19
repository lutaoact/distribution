package cache

import (
	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/context"
)

func reqEntry(c context.Context) *logrus.Entry {
	reqid, _ := c.Value("trace.id").(string)
	return logrus.WithField("reqid", reqid)
}
