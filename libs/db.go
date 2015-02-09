package libs

import (
	"fmt"
	"log"

	"github.com/Unknwon/goconfig"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
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

	cfg, err := goconfig.LoadConfigFile("conf/config.ini")
	if err != nil {
		log.Printf("配置文件没有找到，err:%s", err)
	}
	runmode, err := cfg.GetValue(goconfig.DEFAULT_SECTION, "runmode")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	maxIdleConn := cfg.MustInt(runmode, "max_idle_conn")
	maxOpenConn := cfg.MustInt(runmode, "max_open_conn")

	dbUser, err := cfg.GetValue(runmode, "master_user")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbPass, err := cfg.GetValue(runmode, "master_pass")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbHost, err := cfg.GetValue(runmode, "master_host")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbPort, err := cfg.GetValue(runmode, "master_port")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	dbName, err := cfg.GetValue(runmode, "master_name")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}

	dbLink := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4", dbUser, dbPass, dbHost, dbPort, dbName) + "&loc=Asia%2FChongqing"

	if err := orm.RegisterDataBase("default", "mysql", dbLink, maxIdleConn, maxOpenConn); err != nil {
		panic(fmt.Errorf("无法连接Master数据库%v", err))
	}

	slave1dbUser, err := cfg.GetValue(runmode, "slave1_user")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbPass, err := cfg.GetValue(runmode, "slave1_pass")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbHost, err := cfg.GetValue(runmode, "slave1_host")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbPort, err := cfg.GetValue(runmode, "slave1_port")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave1dbName, err := cfg.GetValue(runmode, "slave1_name")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}

	slave1dbLink := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4", slave1dbUser, slave1dbPass, slave1dbHost, slave1dbPort, slave1dbName) + "&loc=Asia%2FChongqing"

	if err := orm.RegisterDataBase("slave1", "mysql", slave1dbLink, maxIdleConn, maxOpenConn); err != nil {
		panic(fmt.Errorf("无法连接Slave数据库%v", err))
	}

}
