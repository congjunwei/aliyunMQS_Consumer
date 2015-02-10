package queues

import (
	"encoding/xml"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/congjunwei/aliyunMQS_Consumer/libs"
	"github.com/congjunwei/aliyunMQS_Consumer/models"

	"github.com/Unknwon/goconfig"
	"github.com/congjunwei/aliyunMQS"
)

type CreateTopicQueue struct {
	BaseQueue
}

func (this *CreateTopicQueue) Run() {
	var msg aliyunMQS.Message
	var object xmlMessage
	//var accesskey, accesssecret, queueownid, mqsurl string
	queuename := "createtopic"
	waitseconds := 30

	cfg, err := goconfig.LoadConfigFile("conf/config.ini")
	if err != nil {
		log.Printf("配置文件没有找到，err:%s", err)
	}
	accessKey, err := cfg.GetValue(goconfig.DEFAULT_SECTION, "accessKey")
	if err != nil {
		log.Printf("配置错误,accessKey,err:%s", err)
	}
	accessSecret, err := cfg.GetValue(goconfig.DEFAULT_SECTION, "accessSecret")
	if err != nil {
		log.Printf("配置错误,accessSecret,err:%s", err)
	}
	queueOwnId, err := cfg.GetValue(goconfig.DEFAULT_SECTION, "queueOwnId")
	if err != nil {
		log.Printf("配置错误,queueOwnId,err:%s", err)
	}
	mqsUrl, err := cfg.GetValue(goconfig.DEFAULT_SECTION, "mqsUrl")
	if err != nil {
		log.Printf("配置错误,mqsUrl,err:%s", err)
	}

	msg.NewMQS(accessKey, accessSecret, queueOwnId, mqsUrl)
	//go func() {
	log.Printf("createtopic消费....")
	for {
		if content, err := msg.ReceiveMessage(queuename, waitseconds); err != nil {
			log.Printf("err:%v,content:%s", err, content)
		} else {
			log.Printf("content:%s", content)
			err := xml.Unmarshal([]byte(content), &object)
			if err != nil {
				log.Printf("读取消息队列返回xml解析失败,content:%s", content)
			}
			receipthandle := object.ReceiptHandle
			body := object.MessageBody
			dequeuecount := object.DequeueCount
			if dequeuecount > 1 { //已被消费过了
				log.Printf("本消息[receipthandle:%s]已被消费过%d次了", receipthandle, dequeuecount)
				continue
			}
			if err := this.process(body); err == nil {
				if content, err := msg.DeleteMessage(queuename, receipthandle); err != nil {
					log.Printf("删除消息失败,receipthandle:%s,content:%s", receipthandle, content)
				} else {
					log.Printf("成功")
				}
			} else {
				log.Printf("处理消息失败,receipthandle:%s", receipthandle)
			}
		}
		log.Printf("本消息处理完毕了！！！！")
	}
	//	}()
}

// @Title 处理信息体
func (this *CreateTopicQueue) process(messagebody string) error {
	var uid, topicid, ctime int
	var err error
	msgbody := strings.Split(messagebody, "|")

	if uid, err = strconv.Atoi(msgbody[0]); err != nil {
		return err
	}
	if topicid, err = strconv.Atoi(msgbody[1]); err != nil {
		return err
	}
	if ctime, err = strconv.Atoi(msgbody[2]); err != nil {
		return err
	}

	var fans models.Fans
	if list := fans.GetFansList(uid); len(list) > 0 {
		for _, v := range list {
			if err := this.AddInboxCache(v, topicid, ctime); err != nil {
				return err
			}
		}
	}
	return nil
}

var inboxThreshold = 500

// @Title 添加单个用户的Inbox
func (this *CreateTopicQueue) AddInboxCache(uid int, topicid int, timestamp int) error {
	cachename := libs.CacheKey(fmt.Sprintf("Inbox#%d", uid))
	cacheinboxs := libs.NewSortedSet(cachename)

	if err := cacheinboxs.ZAdd(timestamp, topicid); err != nil {
		log.Printf("cache[%s]设置失败[%+v]", cachename)
		return err
	} else {
		log.Printf("Inbox#%dcache添加了topicid:%d,timestamp:%d", uid, topicid, timestamp)
		//查看目标key内的value数量,cache内容超过阀值，添加成功后，删除时间最早的（即分数从小到大排列的第一个）
		if count := cacheinboxs.Size(); count > inboxThreshold {
			if err := cacheinboxs.RemoveSoreteSet(0, 0); err != nil {
				log.Printf(fmt.Sprintf("cache[%s]阀值外数据删除失败[%+v]", cachename, err))
				return err
			} else {

			}
		}
		return nil
	}
}
