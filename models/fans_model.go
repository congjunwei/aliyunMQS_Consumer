package models

import (
	"fmt"
	"github.com/congjunwei/aliyunMQS_Consumer/libs"
	//"log"
	"strconv"
	"strings"

	"github.com/astaxie/beego/orm"
)

type Fans struct {
	Id     int  `orm:"size(11);auto;pk"`
	Userid int  `orm:"size(11)"`
	Fansid int  `orm:"size(11)"`
	Status byte `orm:"size(1)"`
	Utime  int  `orm:"size(11)"`
	Ctime  int  `orm:"size(11)"`
}

func (this *Fans) TableName() string {
	return TableName("fans")
}

func (this *Fans) SQLInsert(suffixfactor string, object Fans) (int64, error) {
	o := orm.NewOrm()
	//suffix := MakeSuffixHashName(suffixfactor)
	sql := fmt.Sprintf("INSERT INTO %s(`userid`,`fansid`,`status`,`utime`,`ctime`) VALUES(%d, %d, %d, %d, %d)",
		fmt.Sprintf("%s_%s", this.TableName(), suffixfactor), object.Userid, object.Fansid, object.Status, object.Utime, object.Ctime)
	if res, err := o.Raw(sql).Exec(); err != nil {
		return 0, err
	} else {
		num, _ := res.RowsAffected()
		return num, nil
	}
}
func (this *Fans) SQLDelete(suffixfactor, where string) (int64, error) {
	o := orm.NewOrm()
	//suffix := MakeSuffixHashName(suffixfactor)
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", fmt.Sprintf("%s_%s", this.TableName(), suffixfactor), where)
	if res, err := o.Raw(sql).Exec(); err != nil {
		return 0, err
	} else {
		num, _ := res.RowsAffected()
		return num, nil
	}
}
func (this *Fans) SQLUpdate(suffixfactor, where string, object Fans) (int64, error) {
	o := orm.NewOrm()
	//suffix := MakeSuffixHashName(suffixfactor)
	sql := fmt.Sprintf("UPDATE %s SET `status`=%d WHERE %s", fmt.Sprintf("%s_%s", this.TableName(), suffixfactor), object.Status, where)
	if res, err := o.Raw(sql).Exec(); err != nil {
		return 0, err
	} else {
		num, _ := res.RowsAffected()
		return num, nil
	}
}
func (this *Fans) SQLGetOne(suffixfactor, where string, fields []string) (Fans, error) {
	o := orm.NewOrm()
	o.Using("slave1")
	//suffix := MakeSuffixHashName(suffixfactor)
	var row Fans
	sfields := strings.Join(fields, ",")
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s", sfields, fmt.Sprintf("%s_%s", this.TableName(), suffixfactor), where)
	if err := o.Raw(sql).QueryRow(&row); err != nil {
		return row, err
	} else {
		return row, nil
	}
}
func (this *Fans) SQLGetList(fields []string, suffixfactor, where string, order string, limit, offset int) ([]Fans, error) {
	o := orm.NewOrm()
	o.Using("slave1")
	//suffix := MakeSuffixHashName(suffixfactor)
	var list []Fans
	sfields := strings.Join(fields, ",")
	if limit == -1 {
		limit = 9999
	}
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d, %d", sfields, fmt.Sprintf("%s_%s", this.TableName(), suffixfactor), where, order, offset, limit)
	if num, err := o.Raw(sql).QueryRows(&list); err != nil {
		return list, err
	} else if num == 0 {
		return list, fmt.Errorf("没有找到记录")
	} else {
		return list, nil
	}
}
func (this *Fans) SQLGetCount(suffixfactor, where string) int {
	o := orm.NewOrm()
	o.Using("slave1")
	//suffix := MakeSuffixHashName(suffixfactor)

	sql := fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE %s", fmt.Sprintf("%s_%s", this.TableName(), suffixfactor), where)
	var maps []orm.Params
	if num, err := o.Raw(sql).Values(&maps); err == nil && num > 0 {
		if c, err := strconv.Atoi(maps[0]["cnt"].(string)); err != nil {
			return 0
		} else {
			return c
		}
	} else {
		return 0
	}
}
func (this *Fans) SQLIsExist(suffixfactor, where string) bool {
	o := orm.NewOrm()
	o.Using("slave1")
	//suffix := MakeSuffixHashName(suffixfactor)

	sql := fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE %s", fmt.Sprintf("%s_%s", this.TableName(), suffixfactor), where)
	var maps []orm.Params
	if num, err := o.Raw(sql).Values(&maps); err == nil && num > 0 {
		if c, err := strconv.Atoi(maps[0]["cnt"].(string)); err != nil {
			return false
		} else if c > 0 {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

//取出fans表数据放到缓存中
func (this *Fans) GetFansToCache(userid int) bool {
	o := orm.NewOrm()
	o.Using("slave1")
	suffixfactor := MakeSuffixHashName(fmt.Sprintf("%d", userid))
	where := fmt.Sprintf("userid=%d", userid)
	fields := []string{"fansid"}
	if fans, err := this.SQLGetList(fields, suffixfactor, where, "id", -1, 0); err == nil {
		cache := libs.NewSet(libs.CacheKey(fmt.Sprintf("My_Fans#%d", userid)))
		for _, v := range fans {
			cache.Sadd(v.Fansid)
		}
		return true
	} else {
		return false
	}
}

//返回我的粉丝id
func (this *Fans) GetFansList(userid int) []int {
	//return []int{10005}
	//获取用户的粉丝编号
	var res []int

	cachename := libs.CacheKey(fmt.Sprintf("My_Fans#%d", userid))
	cachemyfans := libs.NewSet(cachename) //确定fans缓存键值

	if fansidlist, err := cachemyfans.Smembers(); err == nil {

		for _, v := range fansidlist {
			i, _ := strconv.Atoi(v)
			res = append(res, i)
		}
		return res
	} else { //缓存没查到查数据库
		return []int{}
		if b := this.GetFansToCache(userid); b {
			cachemyfans := libs.NewSet(cachename) //确定fans缓存键值
			if fansidlist, err := cachemyfans.Smembers(); err == nil {
				for _, v := range fansidlist {
					i, _ := strconv.Atoi(v)
					res = append(res, i)
				}
			}
		}
		return res
	}
}
