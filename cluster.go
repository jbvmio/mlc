package mlc

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

type Cluster struct {
	*memberlist.Memberlist
	ctx           context.Context
	stop          context.CancelFunc
	wg            sync.WaitGroup
	d             Delegate
	n             *Node
	nm            *NodeMeta
	am            *AppMeta
	eventChan     chan memberlist.NodeEvent
	notifications chan []byte
	notifyIsNil   bool
	notifyFunc    func(*memberlist.Node, []byte) error
	Tasks         []Task
}

func NewCluster(N *Node, appData []byte) *Cluster {
	eventChan := make(chan memberlist.NodeEvent, 1000)
	nm := NodeMeta{
		Name: N.Config.Name,
		Data: appData,
	}
	am := CreateAppMeta(nm.Name)
	am.Set(nm.Data)
	d := newDelegate(&nm, am)
	N.Config.Delegate = d
	N.Config.Events = &memberlist.ChannelEventDelegate{Ch: eventChan}
	ctx, cancel := context.WithCancel(context.Background())
	return &Cluster{
		ctx:         ctx,
		stop:        cancel,
		eventChan:   eventChan,
		wg:          sync.WaitGroup{},
		d:           d,
		n:           N,
		nm:          &nm,
		am:          am,
		notifyIsNil: true,
	}
}

func (C *Cluster) Connect(peers ...string) error {
	C.notifyFunc = C.notifyCheck
	C.wg.Add(1)
	go C.listen()
	list, err := memberlist.Create(C.n.Config)
	if err != nil {
		return fmt.Errorf("[ERROR] mlc: unable to create memberlist: %w", err)
	}
	C.Memberlist = list
	C.nm.Address = list.LocalNode().Address()
	var filtered []string
	ap := list.LocalNode().FullAddress()
	np := ap.Name + `:` + strconv.Itoa(C.n.Config.AdvertisePort)
	for _, p := range peers {
		if p != ap.Addr && p != ap.Name && p != np {
			filtered = append(filtered, p)
		}
	}
	mems, err := list.Join(filtered)
	switch {
	case err != nil && mems == 0:
		C.n.L.Printf("[INFO] mlc: Starting Single Node Cluster")
		C.electLeader()
		C.n.L.Printf("[INFO] mlc: executing bootstrap tasks\n")
		for _, task := range C.Tasks {
			err := task(Bootstrap, C)
			if err != nil {
				C.n.L.Printf("[ERROR] mlc: error executing bootstrap task: %v\n", err)
			}
		}
	case mems > 0:
		var count int
		list.LocalNode().Meta = C.d.NodeMeta(MetaMaxSize)
		for C.nm.Leader == "" || C.nm.LeaderAddr == "" {
			C.n.L.Printf("[INFO] mlc: Waiting for Leader Data")
			time.Sleep(time.Second * 1)
			count++
			if count >= 30 {
				err := fmt.Errorf("timed out waiting for leader data")
				C.n.L.Printf("[ERROR] mlc: %v\n", err)
				return err
			}
		}
		C.runOnJoin()
	default:
		return err
	}
	return nil
}

func (C *Cluster) WithNotificationChannel(notificationChannel chan []byte) {
	C.notifyIsNil = true
	C.d.WithNotificationChannel(notificationChannel)
	C.notifications = notificationChannel
	C.notifyIsNil = false
}

func (C *Cluster) listen() {
	defer C.wg.Done()
listenLoop:
	for {
		select {
		case <-C.ctx.Done():
			break listenLoop
		case E := <-C.eventChan:
			switch E.Event {
			case memberlist.NodeJoin:
				C.n.L.Printf("[INFO] mlc: Node Joined: %s Current Leader: %s\n", E.Node.Name, C.nm.Leader)
				if E.Node.Name == C.nm.Name {
					C.n.L.Printf("[INFO] mlc: I'm Joining the Cluster")
				}
			case memberlist.NodeLeave:
				C.n.L.Printf("[INFO] mlc: Node Left: %s Current Leader: %s\n", E.Node.Name, C.nm.Leader)
				C.am.del(E.Node.Name)
				switch E.Node.Name {
				case C.nm.Name:
					C.n.L.Printf("[INFO] mlc: I'm Leaving the Cluster")
					switch {
					case C.nm.Name == C.nm.Leader:
						C.n.L.Printf("[INFO] mlc: executing LeaderOnLeave tasks\n")
						for _, task := range C.Tasks {
							err := task(LeaderOnLeave, C)
							if err != nil {
								C.n.L.Printf("[ERROR] mlc: error executing leave task: %v\n", err)
							}
						}
					default:
						C.n.L.Printf("[INFO] mlc: executing FollowerOnLeave tasks\n")
						for _, task := range C.Tasks {
							err := task(FollowerOnLeave, C)
							if err != nil {
								C.n.L.Printf("[ERROR] mlc: error executing leave task: %v\n", err)
							}
						}
					}
				case C.nm.Leader:
					C.n.L.Printf("[INFO] mlc: executing LeaderElection\n")
					C.electLeader()
				}
			case memberlist.NodeUpdate:
				// We Let the Delegate handle all the Syncing
				C.n.L.Printf("[DEBUG] mlc: Node Updated: %s Current Leader: %s\n", E.Node.Name, C.nm.Leader)
			}
		}
	}
}

func (C *Cluster) Disconnect() error {
	err := C.Leave(time.Second * 7)
	C.stop()
	C.wg.Wait()
	return err
}
