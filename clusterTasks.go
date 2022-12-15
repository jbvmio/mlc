package mlc

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

type Task func(ExecutionStage, ClusterFunc) error

type ExecutionStage int

const (
	Bootstrap             ExecutionStage = iota // On initial 1 node cluster start
	OnDemand                                    // OnDemand Tasks, envoked via RunOnDemand or RunTask
	LeaderOnJoin                                // Newly elected Leader Tasks during an election
	LeaderOnLeave                               // Current Leader Tasks when leaving the cluster
	FollowerOnPromotion                         // During an election, Follower Tasks when promoted to Leader before running LeaderOnJoin Tasks
	FollowerOnLeaderLeave                       // During an election, Follower Tasks when not promoted to Leader befere running FollowerOnNewLeader Tasks
	FollowerOnNewLeader                         // During an election, Follower Tasks when a new Leader has been elected
	FollowerOnJoin                              // Follower Tasks when joining the cluster
	FollowerOnLeave                             // Follower Tasks when leaving the cluster
)

// RegisterTasks registers the given Tasks to the Cluster.
func (C *Cluster) RegisterTasks(tasks ...Task) {
	C.Tasks = append(C.Tasks, tasks...)
}

// RunOnDemand runs all registered Tasks with the OnDemand ExecutionTask parameter.
func (C *Cluster) RunOnDemand() {
	for _, task := range C.Tasks {
		err := task(OnDemand, C)
		if err != nil {
			C.n.L.Printf("[ERROR] mlc: error executing leave task: %v\n", err)
		}
	}
}

// RunTask runs the given Task with with the OnDemand ExecutionTask parameter.
func (C *Cluster) RunTask(onDemand Task) error {
	return onDemand(OnDemand, C)
}

func (C *Cluster) runOnJoin() {
	C.n.L.Printf("[INFO] mlc: executing onJoin tasks\n")
	for _, task := range C.Tasks {
		err := task(FollowerOnJoin, C)
		if err != nil {
			C.n.L.Printf("[ERROR] mlc: error executing join task: %v\n", err)
		}
	}
}

func (C *Cluster) electLeader() {
	oldLeader := C.nm.Leader
	oldLeaderAddr := C.nm.LeaderAddr
	if oldLeader != "" && oldLeaderAddr != "" {
		C.n.L.Printf("[INFO] mlc: leader %s with address %s has left, performing election\n", oldLeader, oldLeaderAddr)
	}
	highestNode, highestAddr := C.quickElect()

	if C.n.DirLockPath != "" && C.dirGuard == nil {
		switch highestNode {
		case C.LocalNode().Name:
			var tries int
			var dirLockErr error
			C.dirGuard, dirLockErr = acquireDirectoryLock(C.n.DirLockPath, lockfileName, C.nm.Name, C.nm.Address)
			for dirLockErr != nil {
				C.n.L.Printf("[INFO] mlc: waiting to acquire dirLock\n")
				tries++
				if tries >= 30 {
					break
				}
				switch {
				case !C.dirLockLeaderMatches(oldLeader, oldLeaderAddr):
					C.n.L.Printf("[INFO] mlc: another node has acquired dirLock leader\n")
					highestNode, highestAddr = C.dirLockLeader()
					dirLockErr = nil
				default:
					time.Sleep(time.Second * 1)
					C.dirGuard, dirLockErr = acquireDirectoryLock(C.n.DirLockPath, lockfileName, C.nm.Name, C.nm.Address)
				}
			}
			if dirLockErr != nil {
				C.n.L.Printf("[INFO] mlc: could not acquire or assign dirLock leader, honering dirLock\n")
				C.nm.Leader, C.nm.LeaderAddr = C.dirLockLeader()
				C.LocalNode().Meta = C.d.Encode(C.nm)
				err := C.UpdateNode(time.Second * 5)
				if err != nil {
					C.n.L.Printf("[ERROR] mlc: error updating node: %v\n", err)
				}
				return
			}
			if C.dirLockLeaderMatch() {
				err := syncDir(C.n.DirLockPath)
				if err != nil {
					C.n.L.Printf("[ERROR] mlc: error syncing dirLock: %v\n", err)
				}
			}
		default:
			var tries int
			for !C.dirLockLeaderMatches(highestNode, highestAddr) {
				C.n.L.Printf("[INFO] mlc: waiting for leader dirLock confirmation\n")
				tries++
				if tries >= 30 {
					break
				}
				if !C.dirLockLeaderMatches(highestNode, highestAddr) && !C.dirLockLeaderMatches(oldLeader, oldLeaderAddr) {
					C.n.L.Printf("[INFO] mlc: another node has acquired dirLock leader\n")
					highestNode, highestAddr = C.dirLockLeader()
					break
				}
				time.Sleep(time.Second * 1)
			}
			if !C.dirLockLeaderMatches(highestNode, highestAddr) {
				C.n.L.Printf("[INFO] mlc: could not acquire or assign dirLock leader, honering dirLock\n")
				C.nm.Leader, C.nm.LeaderAddr = C.dirLockLeader()
				C.LocalNode().Meta = C.d.Encode(C.nm)
				err := C.UpdateNode(time.Second * 5)
				if err != nil {
					C.n.L.Printf("[ERROR] mlc: error updating node: %v\n", err)
				}
				return
			}
		}
	}
	C.nm.Leader = highestNode
	C.nm.LeaderAddr = highestAddr
	C.LocalNode().Meta = C.d.Encode(C.nm)
	err := C.UpdateNode(time.Second * 5)
	C.n.L.Printf("[INFO] mlc: %s elected leader: %s at %s\n", C.nm.Name, C.nm.Leader, C.nm.LeaderAddr)
	if err != nil {
		C.n.L.Printf("[ERROR] mlc: error updating node: %v\n", err)
	}
	switch C.nm.Leader {
	case C.LocalNode().Name:
		C.n.L.Printf("[INFO] mlc: executing FollowerOnPromotion tasks\n")
		for _, task := range C.Tasks {
			err := task(FollowerOnPromotion, C)
			if err != nil {
				C.n.L.Printf("[ERROR] mlc: error executing FollowerOnPromotion task: %v\n", err)
			}
		}
		C.n.L.Printf("[INFO] mlc: executing LeaderOnJoin tasks\n")
		for _, task := range C.Tasks {
			err := task(LeaderOnJoin, C)
			if err != nil {
				C.n.L.Printf("[ERROR] mlc: error executing LeaderOnJoin task: %v\n", err)
			}
		}
	default:
		C.n.L.Printf("[INFO] mlc: executing FollowerOnLeaderLeave tasks\n")
		for _, task := range C.Tasks {
			err := task(FollowerOnLeaderLeave, C)
			if err != nil {
				C.n.L.Printf("[ERROR] mlc: error executing FollowerOnLeaderLeave task: %v\n", err)
			}
		}
		C.n.L.Printf("[INFO] mlc: executing FollowerOnNewLeader tasks\n")
		for _, task := range C.Tasks {
			err := task(FollowerOnNewLeader, C)
			if err != nil {
				C.n.L.Printf("[ERROR] mlc: error executing FollowerOnLeaderJoin task: %v\n", err)
			}
		}
	}
}

func (C *Cluster) quickElect() (highestNode, highestAddr string) {
	var highestVal int
	members := C.Members()
	sort.SliceStable(members, func(i, j int) bool {
		return members[i].Name < members[j].Name
	})
	for _, m := range members {
		var val int
		for _, oct := range strings.Split(m.Addr.String(), `.`) {
			o, _ := strconv.Atoi(oct)
			val += o
		}
		if val > highestVal {
			highestNode = m.Name
			highestAddr = m.Address()
			highestVal = val
		}
	}
	return
}
