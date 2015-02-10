package main

import (
	"github.com/congjunwei/aliyunMQS_Consumer/libs"
	"github.com/congjunwei/aliyunMQS_Consumer/queues"
)

func main() {
	libs.InitRedis()

	var dq queues.DeleteTopicQueue
	var cq queues.CreateTopicQueue
	go cq.Run()
	go dq.Run()
	select {}
}
