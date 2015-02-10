package main

import (
	"github.com/congjunwei/aliyunMQS_consumber/libs"
	"github.com/congjunwei/aliyunMQS_consumber/queues"
)

func main() {
	libs.InitRedis()

	var dq queues.DeleteTopicQueue
	var cq queues.CreateTopicQueue
	go cq.Run()
	go dq.Run()
	select {}
}
