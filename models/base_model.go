package models

import (
	"crypto/md5"
	"fmt"
	"log"
	"time"

	"github.com/Unknwon/goconfig"
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
	cfg, err := goconfig.LoadConfigFile("conf/config.ini")
	if err != nil {
		log.Printf("配置文件没有找到，err:%s", err)
	}
	runmode, err := cfg.GetValue(goconfig.DEFAULT_SECTION, "runmode")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}

	mysqlprefix, err := cfg.GetValue(runmode, "mysqlprefix")
	if err != nil {
		log.Printf("配置文件错误11，err:%s", err)
	}
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
