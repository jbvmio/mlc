package mlc

import (
	"sync"
)

type AppMeta struct {
	lock     sync.Mutex
	nodeName string
	appdata  map[string][]byte
}

func CreateAppMeta(nodeName string) *AppMeta {
	return &AppMeta{
		lock:     sync.Mutex{},
		nodeName: nodeName,
		appdata:  make(map[string][]byte),
	}
}

// Set replaces the LocalNode AppData.
func (M *AppMeta) Set(appdata []byte) {
	M.set(M.nodeName, appdata)
}

// Get returns the LocalNode AppData.
func (M *AppMeta) Get() []byte {
	return M.get(M.nodeName)
}

// GetAll return a mapping copy of all AppData by node name, for each node in the cluster.
func (M *AppMeta) GetAll() map[string][]byte {
	M.lock.Lock()
	all := make(map[string][]byte, len(M.appdata))
	for k, v := range M.appdata {
		cp := make([]byte, len(v))
		copy(cp, v)
		all[k] = cp
	}
	M.lock.Unlock()
	return all
}

func (M *AppMeta) set(nodeName string, appdata []byte) {
	M.lock.Lock()
	M.appdata[nodeName] = appdata
	M.lock.Unlock()
}

func (M *AppMeta) get(nodeName string) []byte {
	M.lock.Lock()
	cp := M.appdata[nodeName]
	M.lock.Unlock()
	return cp
}

func (M *AppMeta) del(nodeName string) {
	M.lock.Lock()
	delete(M.appdata, nodeName)
	M.lock.Unlock()
}
