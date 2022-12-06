package mlc

import (
	"fmt"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/memberlist"
)

// ClusterFuncs are functions available for Tasks.
type ClusterFunc interface {
	Leader() (name, address string)
	AmLeader() bool
	OtherMembers() map[*memberlist.Node][]byte
	Notifications() <-chan []byte
	Notify(*memberlist.Node, []byte) error
	NotifyAll([]byte) error
	SetData([]byte)
	GetData() []byte
	ClusterData() map[string][]byte
}

func (C *Cluster) Leader() (name, address string) {
	//meta := C.localMeta()
	//return meta.Leader, meta.LeaderAddr
	return C.nm.Leader, C.nm.LeaderAddr
}

func (C *Cluster) AmLeader() bool {
	//meta := C.localMeta()
	//return meta.Leader == C.LocalNode().Name
	return C.nm.Leader == C.nm.Name
}

// OtherMembers returns a mapping of all other known live nodes along with that node's AppData.
func (C *Cluster) OtherMembers() map[*memberlist.Node][]byte {
	memMap := make(map[*memberlist.Node][]byte)
	for _, m := range C.Members() {
		if m.Name != C.LocalNode().Name {
			v := C.am.get(m.Name)
			cp := *m
			memMap[&cp] = v
		}
	}
	return memMap
}

func (C *Cluster) Notifications() <-chan []byte {
	return C.notifications
}

func (C *Cluster) Notify(member *memberlist.Node, msg []byte) error {
	return C.notifyFunc(member, msg)
}

func (C *Cluster) NotifyAll(msg []byte) error {
	var errs error
	for member := range C.OtherMembers() {
		err := C.notifyFunc(member, msg)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("failed to send to %s: %v", member.Name, err))
		}
	}
	return errs
}

// SetData replaces the LocalNode AppData.
func (C *Cluster) SetData(appdata []byte) {
	C.nm.Data = appdata
	C.am.Set(appdata)
}

// Get returns a copy of the LocalNode AppData.
func (C *Cluster) GetData() []byte {
	return C.am.Get()
}

// ClusterData return a mapping snapshot of all AppData, for each node in the cluster.
// The AppData returned is only a Copy.
func (C *Cluster) ClusterData() map[string][]byte {
	return C.am.GetAll()
}

func (C *Cluster) notify(member *memberlist.Node, msg []byte) error {
	return C.SendReliable(member, msg)
}

func (C *Cluster) notifyCheck(member *memberlist.Node, msg []byte) error {
	C.n.L.Printf("[DEBUG] mlc: Checking NIL Channel\n")
	if C.notifyIsNil {
		panic("notification channel is nil")
	}
	C.notifyFunc = C.notify
	return C.SendReliable(member, msg)
}

/*
func (C *Cluster) localMeta() NodeMeta {
	return C.d.Decode(C.d.NodeMeta(MetaMaxSize))
}
*/
