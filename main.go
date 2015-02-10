package main

import (
	"github.com/congjunwei/aliyunMQS_Consumber/libs"
	"github.com/congjunwei/aliyunMQS_Consumber/queues"
)

func main() {
	libs.InitRedis()

	var dq queues.DeleteTopicQueue
	var cq queues.CreateTopicQueue
	go cq.Run()
	go dq.Run()
	select {}
}
