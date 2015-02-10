package models

import (
	"crypto/md5"
	"fmt"
	"log"
	"time"

	"github.com/congjunwei/aliyunMQS_consumber/libs/config"

	"github.com/astaxie/beego/orm"
)

type BaseModel struct {
}

func init() {
	//注册model(每个Model都需要在此注册)
	orm.RegisterModel(new(Fans))
}

//返回带前缀的表名
func TableName(str string) string {
	mysqlprefix, err := config.Cfg.GetValue(config.Runmode, "mysqlprefix")
	if err != nil {
		log.Printf("配置文件错误11，err:%s", err)
	}

	//log.Printf("mysqlprefix:%s", mysqlprefix)
	return fmt.Sprintf("%s%s", mysqlprefix, str)
}

func NowUnixTime() int {
	return int(time.Now().Unix())
}

func MakeSuffixHashName(str string) string {
	b := fmt.Sprintf("%d", str)
	result := fmt.Sprintf("%x", md5.Sum([]byte(b)))
	result = string(result[0])
	return result
}
