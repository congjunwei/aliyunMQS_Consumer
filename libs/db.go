package libs

import (
	"fmt"
	"log"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"

	"aliyunMQS_consumber/libs/config"
)

//var Cfg = beego.AppConfig

func Initdb() {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalf("err:%s", err)
		}
	}()
	if err := orm.RegisterDriver("mysql", orm.DR_MySQL); err != nil {
		panic(fmt.Errorf("无法注册MySQL驱动"))
	}
	maxIdleConn := config.CfgGetInt("max_idle_conn")
	maxOpenConn := config.CfgGetInt("max_open_conn")

	dbUser, err := config.CfgGetString("master_user")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbPass, err := config.CfgGetString("master_pass")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbHost, err := config.CfgGetString("master_host")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbPort, err := config.CfgGetString("master_port")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbName, err := config.CfgGetString("master_name")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}

	dbLink := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4", dbUser, dbPass, dbHost, dbPort, dbName) + "&loc=Asia%2FChongqing"

	if err := orm.RegisterDataBase("default", "mysql", dbLink, maxIdleConn, maxOpenConn); err != nil {
		panic(fmt.Errorf("无法连接Master数据库%v", err))
	}

	slave1dbUser, err := config.CfgGetString("slave1_user")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbPass, err := config.CfgGetString("slave1_pass")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbHost, err := config.CfgGetString("slave1_host")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbPort, err := config.CfgGetString("slave1_port")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbName, err := config.CfgGetString("slave1_name")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}

	slave1dbLink := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4", slave1dbUser, slave1dbPass, slave1dbHost, slave1dbPort, slave1dbName) + "&loc=Asia%2FChongqing"

	if err := orm.RegisterDataBase("slave1", "mysql", slave1dbLink, maxIdleConn, maxOpenConn); err != nil {
		panic(fmt.Errorf("无法连接Slave数据库%v", err))
	}

}
