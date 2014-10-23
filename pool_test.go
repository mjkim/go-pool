package pool
import (
  "testing"
  "github.com/stretchr/testify/assert"
  "time"
  "os"
  log "github.com/Sirupsen/logrus"
)

func init() {
  log.SetOutput(os.Stderr)
  log.SetLevel(log.DebugLevel)
  log.SetFormatter(&log.TextFormatter{})
}

type IntegerDataSource struct {
  array [128]int
  size int
}

func (self *IntegerDataSource) CreateObject() interface{} {
  size := self.size
  self.size += 1
  self.array[size] = size
  return self.array[size]
}

func (self *IntegerDataSource) IsValidObject(object interface{}) bool {
  obj := object.(int)
  return obj != 0
}

func (self *IntegerDataSource) DestroyObject(object interface{}) {
}

func TestBasic(t *testing.T) {
  config := NewConfig()
  pool := NewPool(&IntegerDataSource{}, config)

  list := make([]interface{}, 0)
  for i := 1; i <= 3; i++ {
    obj := pool.BorrowObject()
    assert.Equal(t, i, obj)
    list = append(list, obj)
  }

  for i := range list {
    obj := list[i]
    pool.ReturnObject(obj)
  }

  assert.Equal(t, 3, pool.BorrowObject())
  assert.Equal(t, 2, pool.BorrowObject())
  assert.Equal(t, 1, pool.BorrowObject())

  obj := pool.BorrowObject()
  assert.Equal(t, obj, 4)
  pool.ReturnObject(obj)
  assert.Equal(t, pool.BorrowObject(), 4)
}

func TestWaitIdleObject(t *testing.T) {
  config := NewConfig()
  pool := NewPool(&IntegerDataSource{}, config)

  list := make([]interface{}, 0)
  for i := 1 ; i <= pool.config.MaxActive ; i++ {
    obj := pool.BorrowObject()
    assert.Equal(t, i, obj)
    list = append(list, obj)
  }
  go func() {
    time.Sleep(10 * time.Millisecond)
    var obj interface{}
    obj, list = list[0], list[1:len(list)]
    pool.ReturnObject(obj)
  }()
  assert.Equal(t, 1, pool.BorrowObject())
}
