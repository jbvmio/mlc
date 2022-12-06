package mlc

import (
	"github.com/hashicorp/memberlist"
)

var MetaMaxSize = memberlist.MetaMaxSize

type NodeMeta struct {
	Name       string
	Address    string
	Leader     string
	LeaderAddr string
	Data       []byte
}

type Delegate interface {
	memberlist.Delegate
	EncoderDecoder
	WithNotificationChannel(chan []byte)
}

type delegate struct {
	nodeMeta      *NodeMeta
	appMeta       *AppMeta
	ed            EncoderDecoder
	notifications chan []byte
}

func newDelegate(meta *NodeMeta, appMeta *AppMeta) Delegate {
	return &delegate{
		nodeMeta: meta,
		appMeta:  appMeta,
		ed:       &DefaultEncoderDecoder{},
	}
}

func (d *delegate) Encode(m *NodeMeta) []byte {
	return d.ed.Encode(m)
}

func (d *delegate) Decode(data []byte) NodeMeta {
	return d.ed.Decode(data)
}

func (d *delegate) WithNotificationChannel(notificationChannel chan []byte) {
	if notificationChannel == nil {
		panic("notification channel is nil")
	}
	d.notifications = notificationChannel
}

func (d *delegate) NodeMeta(limit int) []byte {
	d.nodeMeta.Data = d.appMeta.Get()
	return d.Encode(d.nodeMeta)
}

// GetBroadcasts is called when user data messages can be broadcast.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return [][]byte(nil)
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information.
func (d *delegate) LocalState(join bool) []byte {
	d.nodeMeta.Data = d.appMeta.Get()
	return d.Encode(d.nodeMeta)
}

// MergeRemoteState is invoked after a TCP Push/Pull.
func (d *delegate) MergeRemoteState(data []byte, join bool) {
	meta := d.Decode(data)
	if meta.Leader != "" && d.nodeMeta.Leader == "" {
		d.nodeMeta.Leader = meta.Leader
		d.nodeMeta.LeaderAddr = meta.LeaderAddr
	}
	d.appMeta.set(meta.Name, meta.Data)
}

// NotifyMsg is called when a user-data message is received.
func (d *delegate) NotifyMsg(msg []byte) {
	d.notifications <- msg
}
