package pool

import (
  "sync"
  "time"
  log "github.com/Sirupsen/logrus"
)

type DataSource interface {
  CreateObject() interface{}
  IsValidObject(object interface{}) bool
  DestroyObject(object interface{})
}

type Config struct {
  MaxActive int
  MaxIdle int
  WaitForIdle bool
  TestOnBorrow bool
  TestOnReturn bool
}

func NewConfig() Config {
  return Config {
    MaxActive: 8,
    MaxIdle: 8,
    WaitForIdle: true,
    TestOnBorrow: true,
    TestOnReturn: true,
  }
}

type PooledObject struct {
  object interface{}
  last_used time.Time
}

type Pool struct {
  datasource DataSource
  config Config

  lock sync.Mutex

  active_list map[interface{}]*PooledObject
  idle_list []*PooledObject
}

func NewPool(datasource DataSource, config Config) Pool {
  idle_pool_cap := config.MaxIdle
  if idle_pool_cap < 0 {
    idle_pool_cap = 65535
  }

  if config.MaxActive < 0 {
    config.MaxActive = 65535
  }

  pool := Pool{datasource: datasource, config: config}

  pool.idle_list = make([]*PooledObject, 0, idle_pool_cap)
  pool.active_list = make(map[interface{}]*PooledObject)
  return pool
}

func (pool* Pool) tryBorrowObject() interface{} {
  pool.lock.Lock()
  defer pool.lock.Unlock()

  var pooled_obj *PooledObject;
  if len(pool.idle_list) == 0 && len(pool.active_list) < pool.config.MaxActive {
    obj := pool.datasource.CreateObject()
    pooled_obj = &PooledObject{object: obj}
  } else if len(pool.idle_list) > 0 {
    idle_size := len(pool.idle_list)
    pooled_obj = pool.idle_list[idle_size-1]
    pool.idle_list = pool.idle_list[0:idle_size-1]
  } else {
    log.Debugf("cannot create object idle_list:%d active_list:%d %+v", len(pool.idle_list), len(pool.active_list), pool.config)
    return nil
  }
  obj := pooled_obj.object
  pool.active_list[obj] = pooled_obj
  log.Debugf("tryBorrowObject success %+v", obj)

  return obj
}

func (pool* Pool) removeFromActiveList(object interface{}) {
  pool.lock.Lock()
  defer pool.lock.Unlock()
  delete(pool.active_list, object)
}

func (pool* Pool) removeFromIdleList(object interface{}) {
  pool.lock.Lock()
  defer pool.lock.Unlock()

  for i := range(pool.idle_list) {
    if pool.idle_list[i] == object {
      pool.idle_list = append(pool.idle_list[:i], pool.idle_list[i+1:]...)
    }
  }
}

func (pool* Pool) BorrowObject() interface{} {

  var obj interface{}
  for true {
    obj = pool.tryBorrowObject()
    if obj == nil {
      if pool.config.WaitForIdle {
        time.Sleep(1 * time.Millisecond)
        log.Debugf("BorrowObject wait for idle object")
        continue
      } else {
        panic("not implement!")
      }
    } else {
      if pool.config.TestOnBorrow {
        if !pool.datasource.IsValidObject(obj) {
          log.Debugf("InvalidObject", obj)
          pool.datasource.DestroyObject(obj)
          pool.removeFromActiveList(obj)
          continue
        }
      }
      break
    }
  }

  log.Debugf("BorrowObject#{active_list:%d idle_list:%d}", len(pool.active_list), len(pool.idle_list))
  return obj
}

func (pool *Pool) ReturnObject(object interface{}) {
  pool.lock.Lock()
  defer pool.lock.Unlock()

  pooled_obj, ok := pool.active_list[object]

  if !ok {
    log.Warn("ReturnObject: NotPooledObject")
    panic("invalid object")
  } else {
    delete(pool.active_list, object)
    if pool.config.TestOnReturn && !pool.datasource.IsValidObject(object) {
      pool.datasource.DestroyObject(object)
      log.Debugf("DestroyObject#{active_list:%d idle_list:%d} : InvalidObject", len(pool.active_list), len(pool.idle_list))
    } else {
      if len(pool.idle_list) < pool.config.MaxIdle {
        pooled_obj.last_used = time.Now()
        pool.idle_list = append(pool.idle_list, pooled_obj)
        log.Debugf("ReturnObject#{active_list:%d idle_list:%d}", len(pool.active_list), len(pool.idle_list))
      } else {
        pool.datasource.DestroyObject(object)
        log.Debugf("DestroyObject#{active_list:%d idle_list:%d} : len(idle_list) > MaxIdle", len(pool.active_list), len(pool.idle_list))
      }
    }
  }
}

func (pool *Pool) InvalidateObject(object interface{}) {
  pool.datasource.DestroyObject(object)
  pool.removeFromActiveList(object)
  pool.removeFromIdleList(object)
}

