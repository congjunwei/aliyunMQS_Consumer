package tests

import (
	"github.com/congjunwei/aliyunMQS_consumber/libs"
	"github.com/congjunwei/aliyunMQS_consumber/queues"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCreateTopic(t *testing.T) {
	libs.InitRedis()
	//libs.Initdb()
	Convey("处理创建槽图队列", t, func() {
		var q *queues.CreateTopicQueue
		q.Run()
	})
}
