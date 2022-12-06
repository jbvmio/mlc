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
	var highestVal int
	var highestNode string
	var highestAddr string
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
	C.nm.Leader = highestNode
	C.nm.LeaderAddr = highestAddr
	C.LocalNode().Meta = C.d.Encode(C.nm)
	err := C.UpdateNode(time.Second * 5)
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
