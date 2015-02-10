package queues

import (
	"encoding/xml"
	"fmt"
	"log"
	"strconv"
	"strings"

	"aliyunMQS_consumber/libs"
	"aliyunMQS_consumber/models"

	"github.com/Unknwon/goconfig"
	"github.com/congjunwei/aliyunMQS"
)

type DeleteTopicQueue struct {
	BaseQueue
}

func (this *DeleteTopicQueue) Run() {
	var msg aliyunMQS.Message
	var object xmlMessage
	//var accesskey, accesssecret, queueownid, mqsurl string
	queuename := "deletetopic"
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
	log.Printf("deletetopic消费....")
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
	//}()
}

// @Title 处理信息体
func (this *DeleteTopicQueue) process(messagebody string) error {
	var uid, topicid int
	var err error
	msgbody := strings.Split(messagebody, "|")

	if uid, err = strconv.Atoi(msgbody[0]); err != nil {
		return err
	}
	if topicid, err = strconv.Atoi(msgbody[1]); err != nil {
		return err
	}

	var fans models.Fans
	if list := fans.GetFansList(uid); len(list) > 0 {
		for _, v := range list {
			if err := this.DelInboxCache(v, topicid); err != nil {
				return err
			}
		}
	}
	return nil
}

// @Title 添加单个用户的Inbox
func (this *DeleteTopicQueue) DelInboxCache(uid int, topicid int) error {
	cachename := libs.CacheKey(fmt.Sprintf("Inbox#%d", uid))
	cache := libs.NewSortedSet(cachename)         //确定缓存键值
	if err := cache.Remove(topicid); err != nil { //将topicid逐一删掉
		log.Printf(fmt.Sprintf("cache[%s]数据删除失败[%+v]", cachename, err))
		return err
	} else {
		log.Printf(fmt.Sprintf("cache[%s]数据删除成功", cachename))
		return nil
	}
}
