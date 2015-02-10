package config

import (
	//"errors"
	"github.com/Unknwon/goconfig"
	"log"
)

//var Config *goconfig.ConfigFile
//var Runmode string

var (
	Cfg     *goconfig.ConfigFile
	Runmode string
)

func init() {
	var err error
	Cfg, err = goconfig.LoadConfigFile("conf/config.ini")
	if err != nil {
		log.Printf("配置文件没有找到，err:%s", err)
	}
	Runmode, err = Cfg.GetValue(goconfig.DEFAULT_SECTION, "runmode")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
}

func CfgGetString(name string) (string, error) {
	return Cfg.GetValue(Runmode, name)
}

func CfgGetInt(name string) int {
	return Cfg.MustInt(Runmode, name)
}
