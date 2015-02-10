package libs

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"aliyunMQS_consumber/libs/config"
)

var master_pool *redis.Pool
var slave_pool *redis.Pool

//组装cache的key
func CacheKey(key string) string {
	runmode := config.Runmode
	return fmt.Sprintf("juzi-caochang_1.0_%s_%s", runmode, key)

}

func InitRedis() {
	master_host, err := config.CfgGetString("master_redishost")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	master_port, err := config.CfgGetString("master_redisport")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	master_pass, err := config.CfgGetString("master_redispassword")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}

	slave_host, err := config.CfgGetString("slave_redishost")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave_port, err := config.CfgGetString("slave_redisport")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}
	slave_pass, err := config.CfgGetString("slave_redispassword")
	if err != nil {
		log.Printf("配置文件错误，err:%s", err)
	}

	mconn := fmt.Sprintf("%s:%s", master_host, master_port)
	mpassword := fmt.Sprintf("%s", master_pass)

	sconn := fmt.Sprintf("%s:%s", slave_host, slave_port)
	spassword := fmt.Sprintf("%s", slave_pass)

	MasterCacheInit(mconn, mpassword)
	SlaveCacheInit(sconn, spassword)
}

func MasterCacheInit(server, password string) {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalf("err:%s", err)
		}
	}()
	master_pool = &redis.Pool{
		MaxIdle: 80,
		//MaxActive:   8,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				panic(fmt.Errorf("无法打开连接MasterRedis%v", err))
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					panic(fmt.Errorf("无法验证连接MasterRedis%v", err))
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func SlaveCacheInit(server, password string) {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalf("err:%s", err)
		}
	}()
	slave_pool = redis.NewPool(
		func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				panic(fmt.Errorf("无法打开连接SlaveRedis%v", err))
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					panic(fmt.Errorf("无法验证连接SlaveRedis%v", err))
				}
			}
			return c, err
		}, 10)
	// slave_pool = &redis.Pool{
	// 	MaxIdle: 80,
	// 	//MaxActive:   8,
	// 	IdleTimeout: 240 * time.Second,
	// 	Dial: func() (redis.Conn, error) {
	// 		c, err := redis.Dial("tcp", server)
	// 		if err != nil {
	// 			panic(fmt.Errorf("无法打开连接SlaveRedis%v", err))
	// 		}
	// 		if password != "" {
	// 			if _, err := c.Do("AUTH", password); err != nil {
	// 				c.Close()
	// 				panic(fmt.Errorf("无法验证连接SlaveRedis%v", err))
	// 			}
	// 		}
	// 		return c, err
	// 	},
	// 	TestOnBorrow: func(c redis.Conn, t time.Time) error {
	// 		_, err := c.Do("PING")
	// 		return err
	// 	},
	//}
}

func MasterRedisInfo() redis.Conn {
	return master_pool.Get()
}

func SlaveRedisInfo() redis.Conn {
	return slave_pool.Get()
}

type HashMap struct {
	Name string
}

func NewHashMap(name string) *HashMap {
	Do("PING")
	return &HashMap{name}
}

//操作主服务器，只负责写数据
func Send(cmd string, args ...interface{}) error {
	red := MasterRedisInfo()
	defer red.Close()
	err := red.Send(cmd, args...)
	//.Debug(fmt.Sprintf("Send %v %v %v", cmd, args, err))
	if err != nil {
		fmt.Sprintf("Cache:%s", err)
		return err
	}
	return red.Flush()
}

//操作从服务器，只负责读数据
func Do(cmd string, args ...interface{}) (interface{}, error) {
	red := SlaveRedisInfo()
	defer red.Close()
	return red.Do(cmd, args...)
}

func DoMaster(cmd string, args ...interface{}) (interface{}, error) {
	red := MasterRedisInfo()
	defer red.Close()
	return red.Do(cmd, args...)
}

func Multi() error {
	if _, err := Do("MULTI"); err == nil {
		return nil
	} else {
		return err
	}
}

// func Exec() ([]string, error) {
// 	if list, err := Do("EXEC"); err == nil {

// 	}
// }

func titleCasedName(name string) string {
	newstr := make([]rune, 0)
	upNextChar := true

	for _, chr := range name {
		switch {
		case upNextChar:
			upNextChar = false
			chr -= ('a' - 'A')
		case chr == '_':
			upNextChar = true
			continue
		}

		newstr = append(newstr, chr)
	}

	return string(newstr)
}
func (this *HashMap) SetExpire(second int) error {
	return Send("EXPIRE", this.Name, second)
}

func (this *HashMap) PutObject(k string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return Send("HSET", this.Name, k, b)
}

func (this *HashMap) GetObjectStruct(object interface{}) error {
	// if this.Exists() == false {
	// 	return fmt.Errorf("cache不存在")
	// }
	if v, err := redis.Values(Do("HGETALL", this.Name)); err != nil {
		return err
	} else if len(v) == 0 {
		return fmt.Errorf("cache是空的")
	} else {
		if err := redis.ScanStruct(v, object); err != nil {
			return err
		} else {
			return nil
		}
	}
}

func (this *HashMap) PutObjectStruct(object interface{}) error {
	if err := Send("HMSET", redis.Args{}.Add(this.Name).AddFlat(object)...); err != nil {
		return err
	} else {
		return nil
	}
}

func (this *HashMap) GetObject(k string, clazz interface{}) error {
	b, err := redis.Bytes(Do("HGET", this.Name, k))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, clazz)
}

func (this *HashMap) Get(k string) (string, error) {
	b, err := redis.Bytes(Do("HGET", this.Name, k))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (this *HashMap) Put(k string, v interface{}) error {
	if this.Exists() == false {
		return fmt.Errorf("cache不存在")
	}
	return Send("HSET", this.Name, k, v)
}

func (this *HashMap) GetString(k string) (string, error) {
	str, err := redis.String(Do("HGET", this.Name, k))
	if err == nil {
		str = strings.Trim(str, "\"")
	}
	return str, err
}

func (this *HashMap) GetStringList(k []string) ([]string, error) {
	args := []interface{}{}
	args = append(args, this.Name)
	for _, v := range k {
		args = append(args, v)
	}
	reply, err := redis.MultiBulk(Do("HMGET", args...))
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range reply {
		s, err := redis.String(v, nil)
		if err != nil {
			break
		}
		s = strings.Trim(s, "\"")
		list = append(list, s)
	}
	return list, err
}

func (this *HashMap) MultiGet(k []string) ([]string, error) {
	args := []interface{}{}
	args = append(args, this.Name)
	for _, v := range k {
		args = append(args, v)
	}
	reply, err := redis.MultiBulk(Do("HMGET", args...))
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range reply {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
}

func (this *HashMap) Size() (int, error) {
	return redis.Int(Do("HLEN", this.Name))
}

func (this *HashMap) Remove(k string) error {
	return Send("HDEL", this.Name, k)
}

func (this *HashMap) HExists(k string) bool {
	v, err := redis.Bool(Do("HEXISTS", this.Name, k))
	if err != nil {
		return false
	}
	return v
}

func (this *HashMap) Exists() bool {
	if b, err := redis.Bool(Do("EXISTS", this.Name)); err != nil {
		return false
	} else if b == false {
		return false
	} else {
		return true
	}
}

func (this *HashMap) Hincrby(field string, increment int) error {
	if this.Exists() == false {
		return fmt.Errorf("cache不存在")
	}
	if err := Send("HINCRBY", this.Name, field, increment); err != nil {
		return err
	} else {
		return nil
	}
}

func (this *HashMap) Clear() error {
	return Send("DEL", this.Name)
}

func Del(k []string) error {
	args := []interface{}{}
	for _, v := range k {
		args = append(args, v)
	}
	if err := Send("DEL", args...); err != nil {
		return err
	} else {
		return nil
	}
}

//有序集合
type SortedSet struct {
	Name string
}

func NewSortedSet(name string) *SortedSet {
	Do("PING")
	return &SortedSet{name}
}

func (this *SortedSet) SetExpire(second int) error {
	return Send("EXPIRE", this.Name, second)
}

func (this *SortedSet) Exists() bool {
	if b, err := redis.Bool(Do("EXISTS", this.Name)); err != nil {
		return false
	} else if b == false {
		return false
	} else {
		return true
	}
}

func (this *SortedSet) AddObject(score float64, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return Send("ZADD", this.Name, score, b)
}

func (this *SortedSet) ZAdd(score interface{}, v interface{}) error {
	return Send("ZADD", this.Name, score, v)
}

func (this *SortedSet) Set(score float64, v string) error {
	return Send("ZADD", this.Name, score, []byte(v))
}

func (this *SortedSet) AddString(score float64, v string) error {
	return Send("ZADD", this.Name, score, v)
}

func (this *SortedSet) Size() int {
	b, err := redis.Int(Do("ZCARD", this.Name))
	if err != nil {
		return -1
	}
	return b
}

func (this *SortedSet) SizeByScore(min, max string) int {
	b, err := redis.Int(Do("ZCOUNT", this.Name, min, max))
	if err != nil {
		return -1
	}
	return b
}

func (this *SortedSet) GetObject(index int, clazz interface{}) error {
	b, err := redis.Bytes(Do("ZRANGE", this.Name, index, index+1))
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, clazz)
	return err
}

func (this *SortedSet) Get(index int) (string, error) {
	b, err := redis.Bytes(Do("ZRANGE", this.Name, index, index+1))
	if err != nil {
		return "", err
	}
	return string(b), err
}

//redis>  ZADD myzset 1 "one"
//(integer) 1
//redis>  ZADD myzset 2 "two"
//(integer) 1
//redis>  ZADD myzset 3 "three"
//(integer) 1
//redis>  ZRANK myzset "three"
//(integer) 2
//redis>  ZRANK myzset "four"
//(nil)
func (this *SortedSet) Zrank(field string) (int, error) {
	v, err := redis.Int(Do("ZRANK", this.Name, field))
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (this *SortedSet) GetSortedSet(start, stop int) ([]string, error) {
	a, err := Do("ZRANGE", this.Name, start, stop)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range b {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
}

//按score倒序取
func (this *SortedSet) ZrevrangebyscoreStart(size int, max, min, start interface{}) ([]string, error) {
	a, err := Do("ZREVRANGEBYSCORE", this.Name, max, min, "LIMIT", start, size)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range b {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
	// slice := []string{"10792", "10791", "10790", "10789", "10788", "10787", "10786", "10785", "10784", "10783"}
	// return slice, nil
}

//按score倒序取
func (this *SortedSet) Zrevrangebyscore(size int, max, min interface{}) ([]string, error) {
	a, err := Do("ZREVRANGEBYSCORE", this.Name, max, min, "LIMIT", 0, size)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range b {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
	// slice := []string{"10792", "10791", "10790", "10789", "10788", "10787", "10786", "10785", "10784", "10783"}
	// return slice, nil
}

//按score正序取
func (this *SortedSet) Zrangebyscore(size int, min, max interface{}) ([]string, error) {
	a, err := Do("ZRANGEBYSCORE", this.Name, min, max, "LIMIT", 0, size)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range b {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
}

func (this *SortedSet) Zscore(m int) (int, error) {
	v, err := redis.Int(Do("ZSCORE", this.Name, m))
	if err != nil {
		return 0, err
	}
	return v, err
}

func (this *SortedSet) RemoveObject(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return Send("ZREM", this.Name, b)
}

func (this *SortedSet) Remove(v interface{}) error {
	return Send("ZREM", this.Name, v)
}

func (this *SortedSet) GetString(index int) (string, error) {
	str, err := redis.String(Do("ZRANGE", this.Name, index, index+1))
	if err == nil {
		str = strings.Trim(str, "\"")
	}
	return str, err
}

func (this *SortedSet) GetAllStrings() ([]string, error) {
	return this.GetStrings(0, -1)
}

func (this *SortedSet) FindAll() ([]string, error) {
	return this.Find(0, -1)
}

func (this *SortedSet) FindAllRev() ([]string, error) {
	return this.FindRev(0, -1)
}

func (this *SortedSet) GetStrings(start, limit int) ([]string, error) {
	a, err := Do("ZRANGE", this.Name, start, start+limit-1)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}

	var list = make([]string, 0)
	for _, v := range b {
		s, err := redis.String(v, nil)
		if err != nil {
			break
		}
		s = strings.Trim(s, "\"")
		list = append(list, s)
	}
	return list, err
}

func (this *SortedSet) Find(start, limit int) ([]string, error) {
	a, err := Do("ZRANGE", this.Name, start, start+limit-1)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}

	var list = make([]string, 0)
	for _, v := range b {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
}

func (this *SortedSet) GetStringsRev(start, limit int) ([]string, error) {
	a, err := Do("ZREVRANGE", this.Name, start, start+limit-1)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}

	var list = make([]string, 0)
	for _, v := range b {
		s, err := redis.String(v, nil)
		if err != nil {
			break
		}
		s = strings.Trim(s, "\"")
		list = append(list, s)
	}
	return list, err
}

func (this *SortedSet) FindRev(start, limit int) ([]string, error) {
	a, err := Do("ZREVRANGE", this.Name, start, start+limit-1)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}

	var list = make([]string, 0)
	for _, v := range b {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
}

func (this *SortedSet) Zrevrange(start, stop int) ([]string, error) {
	a, err := Do("ZREVRANGE", this.Name, start, stop)
	if err != nil {
		return nil, err
	}
	b, err := redis.MultiBulk(a, err)
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range b {
		b, err := redis.Bytes(v, nil)
		if err != nil {
			break
		}
		list = append(list, string(b))
	}
	return list, err
}

func (this *SortedSet) RemoveString(v string) error {
	return Send("ZREM", this.Name, v)
}

func (this *SortedSet) RemoveRange(start, limit int) error {
	return Send("ZREMRANGEBYRANK", this.Name, start, start+limit-1)
}

func (this *SortedSet) RemoveIndex(index int) error {
	return Send("ZREMRANGEBYRANK", this.Name, index, index+1)
}

func (this *SortedSet) RemoveSoreteSet(start, stop int) error {
	return Send("ZREMRANGEBYRANK", this.Name, start, stop)
}

func (this *SortedSet) ObjectScore(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err = Send("ZINCRBY", this.Name, b); err != nil {
		return err
	}
	return nil
}

func (this *SortedSet) StringScore(v string) error {
	if err := Send("ZINCRBY", this.Name, v); err != nil {
		return err
	}
	return nil
}

func (this *SortedSet) Score(v string) error {
	if err := Send("ZINCRBY", this.Name, []byte(v)); err != nil {
		return err
	}
	return nil
}

func (this *SortedSet) Clear() error {
	return Send("DEL", this.Name)
}

//-=================string类型
type String struct {
	Name string
}

func (this *String) SetExpire(second int) error {
	return Send("EXPIRE", this.Name, second)
}

func (this *String) Exists() bool {
	if b, err := redis.Bool(Do("EXISTS", this.Name)); err != nil {
		return false
	} else if b == false {
		return false
	} else {
		return true
	}
}

func (this *String) ExistsMaster() bool {
	if b, err := redis.Bool(DoMaster("EXISTS", this.Name)); err != nil {
		return false
	} else if b == false {
		return false
	} else {
		return true
	}
}

func NewString(name string) *String {
	Do("PING")
	return &String{name}
}

func (this *String) Clear() error {
	return Send("DEL", this.Name)
}

func (this *String) Set(v interface{}, ex int) error {
	var err error
	if ex == 0 {
		err = Send("SET", this.Name, v)
	} else {
		err = Send("SET", this.Name, v, "EX", ex)
	}
	if err != nil {
		return err
	}
	return nil
}

func (this *String) Setnx(v interface{}) error {
	err := Send("SETNX", this.Name, v)
	if err != nil {
		return err
	}
	return nil
}

func (this *String) Incrby(increment int) error {
	if err := Send("INCRBY", this.Name, increment); err != nil {
		return err
	}
	return nil
}

func (this *String) GetInt() (int, error) {
	v, err := redis.Int(Do("GET", this.Name))
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (this *String) GetString() (string, error) {
	v, err := redis.String(Do("GET", this.Name))
	if err != nil {
		return "", err
	}
	return v, nil
}

func Mget(k []string) ([]string, error) {
	args := []interface{}{}
	for _, v := range k {
		args = append(args, v)
	}
	reply, err := redis.MultiBulk(Do("MGET", args...))
	if err != nil {
		return nil, err
	}
	var list = make([]string, 0)
	for _, v := range reply {
		s, _ := redis.String(v, nil)
		s = strings.Trim(s, "\"")
		list = append(list, s)
	}
	return list, err
}

//Set集合类型

type Set struct {
	Name string
}

func NewSet(name string) *Set {
	Do("PING")
	return &Set{name}
}

func (this *Set) SetExpire(second int) error {
	return Send("EXPIRE", this.Name, second)
}

func (this *Set) Exists() bool {
	if b, err := redis.Bool(Do("EXISTS", this.Name)); err != nil {
		return false
	} else if b == false {
		return false
	} else {
		return true
	}
}

func (this *Set) Clear() error {
	return Send("DEL", this.Name)
}

//将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
//假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
//当 key 不是集合类型时，返回一个错误
func (this *Set) Sadd(v interface{}) error {
	err := Send("SADD", this.Name, v)
	if err != nil {
		return err
	}
	return nil
}

func (this *Set) SaddList(k []string) error {
	args := []interface{}{}
	args = append(args, this.Name)
	for _, v := range k {
		args = append(args, v)
	}
	if err := Send("SADD", args...); err != nil {
		return err
	} else {
		return nil
	}
}

//返回集合 key 的基数(集合中元素的数量)。
func (this *Set) Scard() (int, error) {
	v, err := redis.Int(Do("SCARD", this.Name))
	if err != nil {
		return 0, err
	}
	return v, nil
}

//返回集合 key 中的所有成员。
func (this *Set) Smembers() ([]string, error) {
	v, err := redis.Strings(Do("SMEMBERS", this.Name))
	if err != nil {
		return v, err
	}
	return v, nil
}

//返回成员 member 是否是存储的集合 key的成员.
//如果member元素是集合key的成员，则返回1
//如果member元素不是key的成员，或者集合key不存在，则返回0
// redis>  SADD myset "one"
// (integer) 1
// redis>  SISMEMBER myset "one"
// (integer) 1
// redis>  SISMEMBER myset "two"
// (integer) 0
func (this *Set) Sismember(v interface{}) (int, error) {
	ret, err := redis.Int(Do("SISMEMBER", this.Name, v))
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (this *Set) Sinter(v string) ([]string, error) {
	ret, err := redis.Strings(Do("SINTER", this.Name, v))
	if err != nil {
		return ret, err
	}
	return ret, nil
}

//在key集合中移除指定的元素. 如果指定的元素不是key集合中的元素则忽略 如果key集合不存在则被视为一个空的集合，该命令返回0.
//如果key的类型不是一个集合,则返回错误.
//redis>  SADD myset "one"
//(integer) 1
//redis>  SADD myset "two"
//(integer) 1
//redis>  SADD myset "three"
//(integer) 1
//redis>  SREM myset "one"
//(integer) 1
//redis>  SREM myset "four"
//(integer) 0
//redis>  SMEMBERS myset
//1) "three"
//2) "two"
func (this *Set) Srem(v interface{}) error {
	err := Send("SREM", this.Name, v)
	if err != nil {
		return err
	}
	return nil
}

//随机返回一个元素
// # 添加元素
// redis> SADD fruit apple banana cherry
// (integer) 3
// # 只给定 key 参数，返回一个随机元素
// redis> SRANDMEMBER fruit
// "cherry"
// redis> SRANDMEMBER fruit
// "apple"
func (this *Set) Srandmember() (int, error) {
	res, err := redis.Int(Do("SRANDMEMBER", this.Name))
	if err != nil {
		return 0, err
	}
	return res, nil
}

//List集合类型

type List struct {
	Name string
}

func NewList(name string) *List {
	Do("PING")
	return &List{name}
}

func (this *List) Lpush(v interface{}) (int, error) {
	if err := Send("LPUSH", this.Name, v); err != nil {
		return 0, err
	} else {
		return 0, nil
	}
}

func (this *List) Brpop(v int) (string, error) {
	if ret, err := redis.Strings(Do("BRPOP", this.Name, v)); err != nil {
		return "", err
	} else {
		return ret[1], nil
	}
}
