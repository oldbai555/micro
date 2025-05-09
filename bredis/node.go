package bredis

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/json"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"google.golang.org/protobuf/proto"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

func New(ip string, port int, username, password string) (g *Group, err error) {
	g = &Group{service: "redis", password: password}
	g.nodeList = append(g.nodeList, newNode(ip, port, username, password))
	if len(g.nodeList) == 0 {
		err = errors.New("not found available redis node")
		log.Errorf("err:%v", err)
		return
	}
	return
}

func toString(val interface{}) (string, error) {
	switch x := val.(type) {
	case bool:
		if x {
			return "1", nil
		}
		return "0", nil
	case int:
		return fmt.Sprintf("%d", x), nil
	case int8:
		return fmt.Sprintf("%d", x), nil
	case int16:
		return fmt.Sprintf("%d", x), nil
	case int32:
		return fmt.Sprintf("%d", x), nil
	case int64:
		return fmt.Sprintf("%d", x), nil
	case uint:
		return fmt.Sprintf("%d", x), nil
	case uint8:
		return fmt.Sprintf("%d", x), nil
	case uint16:
		return fmt.Sprintf("%d", x), nil
	case uint32:
		return fmt.Sprintf("%d", x), nil
	case uint64:
		return fmt.Sprintf("%d", x), nil
	case float32:
		if math.Floor(float64(x)) == float64(x) {
			return fmt.Sprintf("%.0f", x), nil
		}

		return fmt.Sprintf("%f", x), nil
	case float64:
		if math.Floor(x) == x {
			return fmt.Sprintf("%.0f", x), nil
		}

		return fmt.Sprintf("%f", x), nil
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case nil:
		return "", nil
	case error:
		return x.Error(), nil
	// case proto.Message:
	// 	buf, err := utils.Pb2JsonSkipDefaults(x)
	// 	if err != nil {
	// 		log.Errorf("err:%v", err)
	// 		return "", err
	// 	}
	// 	return buf, nil
	default:
		buf, err := json.Marshal(x)
		if err != nil {
			log.Errorf("err:%v", err)
			return "", err
		}

		return string(buf), nil
	}
}

type Node struct {
	client *redis.Client
	ip     string
	port   int
	hash   uint32
}

func hashStr(s string) uint32 {
	f := fnv.New32a()
	_, _ = f.Write([]byte(s))
	return f.Sum32()
}

func newNode(ip string, port int, username, password string) *Node {
	n := new(Node)
	n.ip = ip
	n.port = port
	n.hash = hashStr(fmt.Sprintf("%s:%d", ip, port))
	n.client = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", ip, port),
		Password: password,
		Username: username,
	})
	return n
}

type Group struct {
	nodeList []*Node
	service  string
	password string
	mu       sync.RWMutex
}
type nodeListSorter struct {
	nodeList []*Node
}

func (s *nodeListSorter) Len() int {
	return len(s.nodeList)
}

func (s *nodeListSorter) Less(i, j int) bool {
	return s.nodeList[i].hash < s.nodeList[j].hash
}

func (s *nodeListSorter) Swap(i, j int) {
	tmp := s.nodeList[i]
	s.nodeList[i] = s.nodeList[j]
	s.nodeList[j] = tmp
}

func (g *Group) Service() string {
	return g.service
}

func (g *Group) sortNodes() {
	i := &nodeListSorter{nodeList: g.nodeList}
	sort.Sort(i)
	g.nodeList = i.nodeList
}

func (g *Group) FindClient4Key(key string) *redis.Client {
	hash := hashStr(key)
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.nodeList) == 0 {
		return nil
	}
	i := 0
	for i+1 < len(g.nodeList) {
		if hash >= g.nodeList[i].hash && hash < g.nodeList[i+1].hash {
			return g.nodeList[i].client
		}
		i++
	}
	return g.nodeList[len(g.nodeList)-1].client
}

func (g *Group) GetNodes() []*Node {
	return g.nodeList
}

func (g *Group) GetClient(node *Node) *redis.Client {
	return node.client
}

func (g *Group) Set(key string, val []byte, exp time.Duration) error {
	log.Debugf("redis: Set: key %s exp %v", key, exp)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.Set(context.Background(), key, val, exp).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (g *Group) SetAny(key string, val interface{}) error {
	log.Debugf("redis: Set: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.Set(context.Background(), key, val, 0).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// Deprecated: 这个方法对val的处理有点问题，虽然入参的interface，但是实际上并不支持所有的interface类型
// 后续使用切换至 SetEx 方法使用，入参相同但是处理了 val 不兼容的问题
// 之前用了这个方法的，如果想要切换新的，请注意下兼容问题
func (g *Group) SetWithExpire(key string, val interface{}, expire time.Duration) error {
	log.Debugf("redis: Set: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.Set(context.Background(), key, val, expire).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) SetEx(key string, val interface{}, expire time.Duration) error {
	if expire <= 0 {
		return errors.New("expire must be greater than 0")
	}

	log.Debugf("redis: Set: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}

	buf, err := toString(val)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = node.Set(context.Background(), key, buf, expire).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) SetUint64(key string, val uint64, exp time.Duration) error {
	log.Debugf("redis: Set: key %s exp %v", key, exp)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.Set(context.Background(), key, val, exp).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (g *Group) SetRange(key string, offset int64, value string) error {
	log.Debugf("redis: SetRange: key %s offset", key, offset)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.SetRange(context.Background(), key, offset, value).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) SetPb(key string, pb proto.Message, exp time.Duration) error {
	log.Debugf("redis: SetPb: key %s, exp %v", key, exp)
	val, err := proto.Marshal(pb)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	// 空串这里先不考虑
	if len(val) == 0 {
		return errors.New("unsupported empty value")
	}
	return g.Set(key, val, exp)
}

func (g *Group) SetJson(key string, j interface{}, exp time.Duration) error {
	val, err := json.Marshal(j)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	// 空串这里先不考虑
	if len(val) == 0 {
		return errors.New("unsupported empty value")
	}
	return g.Set(key, val, exp)
}

func (g *Group) HLen(key string) (uint32, error) {
	log.Debugf("redis: HLen: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	v := node.HLen(context.Background(), key)
	err := v.Err()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		log.Errorf("err:%v", err)
		return 0, err
	}
	return uint32(v.Val()), nil
}

func (g *Group) HSetPb(key, subKey string, j proto.Message, exp time.Duration) error {
	log.Debugf("redis: HSetPb: key %s subKey %+v exp %v", key, subKey, exp)
	val, err := proto.Marshal(j)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err = node.HSet(context.Background(), key, subKey, string(val)).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if exp > 0 {
		err = node.Expire(context.Background(), key, exp).Err()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func (g *Group) HSetJson(key, subKey string, j interface{}, exp time.Duration) error {
	log.Debugf("redis: HSetJson: key %s subKey %+v", key, subKey)
	val, err := json.Marshal(j)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err = node.HSet(context.Background(), key, subKey, string(val)).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if exp > 0 {
		err = node.Expire(context.Background(), key, exp).Err()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func (g *Group) HSetJsonNX(key, subKey string, j interface{}, exp time.Duration, setSuccess *bool) error {
	log.Debugf("redis: HSetJsonNX: key %s subKey %+v exp %v", key, subKey, exp)
	val, err := json.Marshal(j)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	res := node.HSetNX(context.Background(), key, subKey, string(val))
	err = res.Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if exp > 0 {
		err = node.Expire(context.Background(), key, exp).Err()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	if setSuccess != nil {
		*setSuccess = res.Val()
	}
	return nil
}

func (g *Group) Get(key string) ([]byte, error) {
	log.Debugf("redis: Get: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	val, err := node.Get(context.Background(), key).Bytes()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return nil, err
	}
	return val, nil
}

func (g *Group) GetPb(key string, pb proto.Message) error {
	log.Debugf("redis: GetPb: key %s", key)
	val, err := g.Get(key)
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return err
	}
	err = proto.Unmarshal(val, pb)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

var ErrRedisException = errors.New("redis exception")
var ErrJsonUnmarshal = lberr.NewErr(6001, "json unmarshal error")

func (g *Group) GetJson(key string, j interface{}) error {
	val, err := g.Get(key)
	if err != nil {
		if err == redis.Nil {
			return redis.Nil
		}
		log.Errorf("err:%v", err)
		return ErrRedisException
	}
	err = json.Unmarshal(val, j)
	if err != nil {
		log.Errorf("err:%v", err)
		return ErrJsonUnmarshal
	}
	return nil
}

func (g *Group) GetUint64(key string) (uint64, error) {
	val, err := g.Get(key)
	if err != nil {
		if err == redis.Nil {
			return 0, redis.Nil
		}
		log.Errorf("err:%v", err)
		return 0, ErrRedisException
	}

	i, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	return uint64(i), nil
}

func (g *Group) GetIntDef(key string, def int) (int, error) {
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.Get(context.Background(), key).Int64()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return def, err
		}
		return def, nil
	}

	return int(val), nil
}

func (g *Group) GetInt64(key string) (int64, error) {
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.Get(context.Background(), key).Int64()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return 0, err
	}

	return val, nil
}

func (g *Group) GetInt64Def(key string, def int64) (int64, error) {
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.Get(context.Background(), key).Int64()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return def, err
		}
		return def, nil
	}

	return val, nil
}

func (g *Group) HGetAll(key string) (map[string]string, error) {
	log.Debugf("redis: HGetAll: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	return node.HGetAll(context.Background(), key).Result()
}

func (g *Group) HKeys(key string) ([]string, error) {
	log.Debugf("redis: HKeys: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	return node.HKeys(context.Background(), key).Result()
}

func (g *Group) HScan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	log.Debugf("redis: HKeys: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, 0, errors.New("not found available redis node")
	}
	return node.HScan(context.Background(), key, cursor, match, count).Result()
}

func (g *Group) ZScan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	log.Debugf("redis: ZKeys: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, 0, errors.New("not found available redis node")
	}
	return node.ZScan(context.Background(), key, cursor, match, count).Result()
}

func (g *Group) HMGetJson(key, subKey string, j interface{}) error {
	log.Debugf("redis: HMGetJson: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	values, err := node.HMGet(context.Background(), key, subKey).Result()
	if err != nil {
		log.Errorf("redis HMGet err:%v", err)
		return ErrRedisException
	}
	if len(values) == 1 {
		v := values[0]
		if v != nil {
			var buf []byte
			if p, ok := v.(string); ok {
				buf = []byte(p)
			} else if p, ok := v.([]byte); ok {
				buf = p
			}
			if buf != nil {
				if len(buf) > 0 {
					err = json.Unmarshal(buf, j)
					if err != nil {
						log.Errorf("err:%v", err)
						return ErrJsonUnmarshal
					}
				}
				return nil
			}
		}
	}
	return redis.Nil
}

// errorKeyList  返回有序列化问题的key
func (g *Group) HMBatchGetJson(key string, m map[string]interface{}) ([]string, error) {
	log.Debugf("redis: HMBatchGetJson: key %s", key)
	if len(m) == 0 {
		return nil, nil
	}
	var subKeys []string
	for k := range m {
		if k != "" {
			subKeys = append(subKeys, k)
		}
	}
	if len(subKeys) == 0 {
		return nil, nil
	}
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	values, err := node.HMGet(context.Background(), key, subKeys...).Result()
	if err != nil {
		log.Errorf("redis HMGet err:%v", err)
		return nil, ErrRedisException
	}
	var errUnmarshalKeyList []string
	for i, v := range values {
		if v != nil {
			var buf []byte
			if p, ok := v.(string); ok {
				buf = []byte(p)
			} else if p, ok := v.([]byte); ok {
				buf = p
			}
			if buf != nil {
				if len(buf) > 0 && i < len(subKeys) {
					subKey := subKeys[i]
					j := m[subKey]
					if j != nil {
						err = json.Unmarshal(buf, j)
						if err != nil {
							log.Errorf("err:%v", err)
							errUnmarshalKeyList = append(errUnmarshalKeyList, subKey)
						}
					}
				}
			}
		}
	}
	if len(errUnmarshalKeyList) > 0 {
		return errUnmarshalKeyList, ErrJsonUnmarshal
	}
	return nil, nil
}

func (g *Group) HDel(key string, subKey ...string) (int64, error) {
	log.Debugf("redis: HDel: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	delNum, err := node.HDel(context.Background(), key, subKey...).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) Del(key string) error {
	log.Debugf("redis: Del: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.Del(context.Background(), key).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) ZAdd(key string, values ...*redis.Z) error {
	log.Debugf("redis: ZAdd: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.ZAdd(context.Background(), key, values...).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) ZCount(key, min, max string) (int64, error) {
	log.Debugf("redis: ZCount: key %s min %s - max %s", key, min, max)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	v := node.ZCount(context.Background(), key, min, max)
	err := v.Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return v.Val(), nil
}

func (g *Group) ZRangeByScore(key string, opt *redis.ZRangeBy) ([]string, error) {
	log.Debugf("redis: ZRangeByScore: key %s opt %v", key, opt)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	members, err := node.ZRangeByScore(context.Background(), key, opt).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return members, nil
}

func (g *Group) ZRangeByScoreWithScores(key string, opt *redis.ZRangeBy) ([]redis.Z, error) {
	log.Debugf("redis: ZRangeByScoreWithScores: key %s opt %v", key, opt)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	resultList, err := node.ZRangeByScoreWithScores(context.Background(), key, opt).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return resultList, nil
}

func (g *Group) ZIncrBy(key string, increment float64, member string) error {
	log.Debugf("redis: ZIncrBy: key %s increment %v member %s", key, increment, member)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	_, err := node.ZIncrBy(context.Background(), key, increment, member).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) ZRange(key string, start, stop int64) ([]string, error) {
	log.Debugf("redis: ZRange: key %s start %v stop %v", key, start, stop)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	members, err := node.ZRange(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return members, nil
}

func (g *Group) ZRangeWithScores(key string, start, stop int64) ([]redis.Z, error) {
	log.Debugf("redis: ZRangeWithScores: key %s start %v stop %v", key, start, stop)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	resultList, err := node.ZRangeWithScores(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return resultList, nil
}

func (g *Group) ZRevRange(key string, start, stop int64) ([]string, error) {
	log.Debugf("redis: ZRevRange: key %s start %v stop %v", key, start, stop)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	members, err := node.ZRevRange(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return members, nil
}

func (g *Group) ZRevRangeWithScores(key string, start, stop int64) ([]redis.Z, error) {
	log.Debugf("redis: ZRevRangeWithScores: key %s start %v stop %v", key, start, stop)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	resultList, err := node.ZRevRangeWithScores(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return resultList, nil
}

func (g *Group) ZRem(key string, members ...interface{}) (int64, error) {
	log.Debugf("redis: ZRem: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	delNum, err := node.ZRem(context.Background(), key, members...).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) ZRemRangeByScore(key string, min, max string) (int64, error) {
	log.Debugf("redis: ZRemRangeByScore: key %s, min: %s, max: %s", key, min, max)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	delNum, err := node.ZRemRangeByScore(context.Background(), key, min, max).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) ZCard(key string) (int64, error) {
	log.Debugf("redis: ZCard: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	num, err := node.ZCard(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return num, nil
}

func (g *Group) SAdd(key string, values ...interface{}) error {
	log.Debugf("redis: SAdd: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.SAdd(context.Background(), key, values...).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) SRem(key string, members ...interface{}) (int64, error) {
	log.Debugf("redis: SRem: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	delNum, err := node.SRem(context.Background(), key, members...).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) SCard(key string) (int64, error) {
	log.Debugf("redis: SCard: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	num, err := node.SCard(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return num, nil
}

func (g *Group) SIsMember(key string, members interface{}) (bool, error) {
	log.Debugf("redis: SIsMember: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return false, errors.New("not found available redis node")
	}
	ok, err := node.SIsMember(context.Background(), key, members).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return ok, nil
}

func (g *Group) SMembers(key string) ([]string, error) {
	log.Debugf("redis: SMembers: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	members, err := node.SMembers(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return members, nil
}

func (g *Group) HIncrBy(key, field string, incr int64) (int64, error) {
	log.Debugf("redis: HIncrBy: key %s field %s incr %d", key, field, incr)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	n, err := node.HIncrBy(context.Background(), key, field, incr).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return n, nil
}

func (g *Group) HIncr(key, field string) (int64, error) {
	log.Debugf("redis: HIncrBy: key %s field %s incr %d", key, field, 1)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	n, err := node.HIncrBy(context.Background(), key, field, 1).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return n, nil
}

func (g *Group) IncrBy(key string, incr int64) (int64, error) {
	log.Debugf("redis: IncrBy: key %s incr %d", key, incr)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	n, err := node.IncrBy(context.Background(), key, incr).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return n, nil
}

func (g *Group) DecrBy(key string, decr int64) (int64, error) {
	log.Debugf("redis: DecrBy: key %s decr %d", key, decr)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	n, err := node.DecrBy(context.Background(), key, decr).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return n, nil
}

func (g *Group) HSet(key, field string, val interface{}) error {
	log.Debugf("redis: HSet: key %s field %s", key, field)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.HSet(context.Background(), key, field, val).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (g *Group) HMSet(key string, fields map[string]interface{}) error {
	log.Debugf("redis: HMSet: key %s fields %v", key, fields)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	err := node.HMSet(context.Background(), key, fields).Err()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (g *Group) HGet(key, subKey string) (string, error) {
	log.Debugf("redis: HGet: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return "", errors.New("not found available redis node")
	}
	val, err := node.HGet(context.Background(), key, subKey).Result()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return "", err
	}
	return val, nil
}

func (g *Group) HGetUint64(key, subKey string) (uint64, error) {
	log.Debugf("redis: HGet: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.HGet(context.Background(), key, subKey).Uint64()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return 0, err
	}
	return val, nil
}

func (g *Group) HGetIntDef(key, subKey string, def int) (int, error) {
	log.Debugf("redis: HGet: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.HGet(context.Background(), key, subKey).Int64()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return def, err
		}
		return def, nil
	}
	return int(val), nil
}

func (g *Group) HGetInt(key, subKey string) (int, error) {
	log.Debugf("redis: HGet: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.HGet(context.Background(), key, subKey).Int64()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return 0, err
	}
	return int(val), nil
}

func (g *Group) HGetJson(key, subKey string, j interface{}) error {
	log.Debugf("redis: HGetJson: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	val, err := node.HGet(context.Background(), key, subKey).Result()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return err
	}
	err = json.Unmarshal([]byte(val), j)
	if err != nil {
		log.Errorf("err:%v", err)
		return ErrJsonUnmarshal
	}
	return nil
}

func (g *Group) HGetPb(key, subKey string, j proto.Message) error {
	log.Debugf("redis: HGetPb: key %s subKey %+v", key, subKey)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	val, err := node.HGet(context.Background(), key, subKey).Result()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return err
	}
	err = proto.Unmarshal([]byte(val), j)
	if err != nil {
		log.Errorf("err:%v", err)
		return ErrJsonUnmarshal
	}
	return nil
}

func (g *Group) Expire(key string, expiration time.Duration) error {
	log.Debugf("redis: Expire: key %s exp %+v", key, expiration)
	node := g.FindClient4Key(key)
	if node == nil {
		return errors.New("not found available redis node")
	}
	_, err := node.Expire(context.Background(), key, expiration).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (g *Group) Exists(key string) (bool, error) {
	log.Debugf("redis: Exists: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return false, errors.New("not found available redis node")
	}
	val, err := node.Exists(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	if val == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (g *Group) HExists(key, field string) (bool, error) {
	log.Debugf("redis: HExists: key %s field %s", key, field)
	node := g.FindClient4Key(key)
	if node == nil {
		return false, errors.New("not found available redis node")
	}
	exists, err := node.HExists(context.Background(), key, field).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return exists, nil
}

func (g *Group) ScriptRun(lua string, keys []string, args ...interface{}) (interface{}, error) {
	node := g.FindClient4Key(keys[0])
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	script := redis.NewScript(lua)
	result, err := script.Run(context.Background(), node, keys, args...).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return result, nil
}

func (g *Group) EvalSha(luaSha1 string, keys []string, args ...interface{}) (interface{}, error) {
	node := g.FindClient4Key(keys[0])
	if node == nil {
		return nil, errors.New("not found available redis node")
	}
	result, err := node.EvalSha(context.Background(), luaSha1, keys, args...).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return result, nil
}

func (g *Group) ScriptLoad(luaScript string) (string, error) {
	node := g.FindClient4Key("")
	if node == nil {
		return "", errors.New("not found available redis node")
	}
	luaSha1, err := node.ScriptLoad(context.Background(), luaScript).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}
	return luaSha1, nil
}

func (g *Group) Incr(key string) (int64, error) {
	log.Debugf("redis: Incr: key %s", key)
	node := g.FindClient4Key("")
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.Incr(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return val, nil
}

func (g *Group) Decr(key string) (int64, error) {
	log.Debugf("redis: Decr: key %s", key)
	node := g.FindClient4Key("")
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	val, err := node.Decr(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return val, nil
}

func (g *Group) ExpireAt(key string, expiredAt time.Time) (bool, error) {
	log.Debugf("redis: ExpireAt: key %s exp %v", key, expiredAt)
	node := g.FindClient4Key(key)
	if node == nil {
		return false, errors.New("not found available redis node")
	}
	ok, err := node.ExpireAt(context.Background(), key, expiredAt).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return ok, nil
}

func (g *Group) LPop(key string) (string, error) {
	log.Debugf("redis: LPop: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return "", errors.New("not found available redis node")
	}
	val, err := node.LPop(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}
	return val, nil
}

func (g *Group) RPop(key string) (string, error) {
	log.Debugf("redis: RPop: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return "", errors.New("not found available redis node")
	}
	val, err := node.RPop(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}
	return val, nil
}

func (g *Group) LPush(key string, values ...interface{}) (int64, error) {
	log.Debugf("redis: LPush: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	count, err := node.LPush(context.Background(), key, values...).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return count, nil
}

func (g *Group) RPush(key string, values ...interface{}) (int64, error) {
	log.Debugf("redis: RPush: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	count, err := node.RPush(context.Background(), key, values...).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return count, nil
}

func (g *Group) LRange(key string, start, stop int64) ([]string, error) {
	log.Debugf("redis: LRange: key %s start %d stop %d", key, start, stop)
	node := g.FindClient4Key(key)
	if node == nil {
		return []string{}, errors.New("not found available redis node")
	}
	result, err := node.LRange(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return []string{}, err
	}
	return result, nil
}

func (g *Group) LTrim(key string, start, stop int64) (string, error) {
	log.Debugf("redis: LTrim: key %s start %d stop %d", key, start, stop)
	node := g.FindClient4Key(key)
	if node == nil {
		return "", errors.New("not found available redis node")
	}
	result, err := node.LTrim(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}
	return result, nil
}

func (g *Group) LLen(key string) (int64, error) {
	log.Debugf("redis: LLen: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	count, err := node.LLen(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return count, nil
}

func (g *Group) LIndex(key string, index int64) (string, error) {
	log.Debugf("redis: LIndex: key %s index %d", key, index)
	node := g.FindClient4Key(key)
	if node == nil {
		return "", errors.New("not found available redis node")
	}
	val, err := node.LIndex(context.Background(), key, index).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}
	return val, nil
}

func (g *Group) SetNX(key string, val []byte, exp time.Duration) (bool, error) {
	log.Debugf("redis: SetNX: key %s exp %v", key, exp)
	node := g.FindClient4Key(key)
	if node == nil {
		return false, errors.New("not found available redis node")
	}
	b, err := node.SetNX(context.Background(), key, val, exp).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return b, nil
}

func (g *Group) SetPbNX(key string, pb proto.Message, exp time.Duration) (bool, error) {
	log.Debugf("redis: SetPbNX: key %s exp %v", key, exp)
	val, err := proto.Marshal(pb)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	b, err := g.SetNX(key, val, exp)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return b, nil
}

func (g *Group) ZScore(key string, member string) (float64, error) {
	log.Debugf("redis: ZScore: key %s member %s", key, member)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	score, err := node.ZScore(context.Background(), key, member).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return score, nil
}

func (g *Group) Ttl(key string) (time.Duration, error) {
	log.Debugf("redis: TTL: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	ttl, err := node.TTL(context.Background(), key).Result()
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return ttl, nil
}

func (g *Group) SetBit(key string, offset int64, val int) (int64, error) {
	log.Debugf("redis: Set Bit: key %s offset %d val %d", key, offset, val)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	intCmd := node.SetBit(context.Background(), key, offset, val)
	if intCmd.Err() != nil {
		log.Errorf("err:%s", intCmd.Err())
		return 0, intCmd.Err()
	}
	return intCmd.Result()
}

func (g *Group) GetBit(key string, offset int64) (int64, error) {
	log.Debugf("redis: Set Bit: key %s offset %d", key, offset)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	intCmd := node.GetBit(context.Background(), key, offset)
	if intCmd.Err() != nil {
		log.Errorf("err:%s", intCmd.Err())
		return 0, intCmd.Err()
	}
	return intCmd.Result()
}

func (g *Group) BitCount(key string, bitCount *redis.BitCount) (int64, error) {
	log.Debugf("redis: BitCount: key %s", key)
	node := g.FindClient4Key(key)
	if node == nil {
		return 0, errors.New("not found available redis node")
	}
	intCmd := node.BitCount(context.Background(), key, bitCount)
	if intCmd.Err() != nil {
		log.Errorf("err:%s", intCmd.Err())
		return 0, intCmd.Err()
	}
	return intCmd.Result()
}

func (g *Group) FlushAll() (string, error) {
	log.Infof("redis: FushAll")
	node := g.FindClient4Key("")
	if node == nil {
		return "", errors.New("not found available redis node")
	}

	intCmd := node.FlushAll(context.Background())
	if intCmd.Err() != nil {
		log.Errorf("err:%s", intCmd.Err())
		return "", intCmd.Err()
	}
	return intCmd.Result()
}

func (g *Group) IsNotFound(err error) bool {
	return err == redis.Nil
}
