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
  Max_active int
  Max_idle int
  Wait_for_idle bool
  Test_on_borrow bool
  Test_on_return bool
}

func NewConfig() Config {
  return Config {
    Max_active: 8,
    Max_idle: 8,
    Wait_for_idle: true,
    Test_on_borrow: true,
    Test_on_return: true,
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
  pool := Pool{datasource: datasource, config: config}
  pool.idle_list = make([]*PooledObject, 0, config.Max_idle)
  pool.active_list = make(map[interface{}]*PooledObject)
  return pool
}

func (pool* Pool) tryBorrowObject() interface{} {
  pool.lock.Lock()
  defer pool.lock.Unlock()

  var pooled_obj *PooledObject;
  if len(pool.idle_list) == 0 && len(pool.active_list) < pool.config.Max_active {
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
      if pool.config.Wait_for_idle {
        log.Debugf("BorrowObject wait for idle object")
        time.Sleep(10 * time.Millisecond)
        continue
      } else {
        panic("not implement!")
      }
    } else {
      if pool.config.Test_on_borrow {
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
    if pool.config.Test_on_return && !pool.datasource.IsValidObject(object) {
      log.Debugf("Return InvalidObject", object)
      pool.datasource.DestroyObject(object)
      log.Debugf("DestroyObject#{active_list:%d idle_list:%d}", len(pool.active_list), len(pool.idle_list))
    } else {
      pooled_obj.last_used = time.Now()
      pool.idle_list = append(pool.idle_list, pooled_obj)

      log.Debugf("ReturnObject#{active_list:%d idle_list:%d}", len(pool.active_list), len(pool.idle_list))
    }
  }
}

func (pool *Pool) InvalidateObject(object interface{}) {
  pool.datasource.DestroyObject(object)
  pool.removeFromActiveList(object)
  pool.removeFromIdleList(object)
}

