package mlc

import (
	"encoding/json"
	"net/http"

	"github.com/hashicorp/memberlist"
)

type StatusResponse struct {
	Name          string               `json:"name"`
	Address       string               `json:"address"`
	Leader        string               `json:"leader"`
	LeaderAddress string               `json:"leaderAddress"`
	Members       []memberlist.Address `json:"members"`
}

func (C *Cluster) ClusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	ml := C.Members()
	members := make([]memberlist.Address, len(ml))
	for i := 0; i < len(ml); i++ {
		members[i] = ml[i].FullAddress()
	}
	statResp := StatusResponse{
		Name:          C.nm.Name,
		Address:       C.nm.Address,
		Leader:        C.nm.Leader,
		LeaderAddress: C.nm.LeaderAddr,
		Members:       members,
	}
	writeJSONResponse(w, http.StatusOK, statResp)
}

func writeJSONResponse(w http.ResponseWriter, statusCode int, obj interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if jsonBytes, err := json.Marshal(obj); err != nil {
		writeJSONError(w, `could not encode JSON`, err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(statusCode)
		w.Write(jsonBytes)
	}
}

func writeJSONError(w http.ResponseWriter, msg string, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(`{"error":true,"message":"` + msg + `","error":"` + err.Error() + `"}`))
}
