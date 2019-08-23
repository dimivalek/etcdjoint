// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdhttp

import (
	"encoding/json"
	"net/http"
	"fmt"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api"
	"github.com/coreos/etcd/lease/leasehttp"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/etcdserver/membership"
)

const (
	peerMembersPrefix = "/members"
	peerLearnersPrefix = "/learners"
)

// NewPeerHandler generates an http.Handler to handle etcd peer requests.
func NewPeerHandler(s etcdserver.ServerPeer) http.Handler {
	return newPeerHandler(s.Cluster(), s.RaftHandler(), s.LeaseHandler())
}

func newPeerHandler(cluster api.Cluster, raftHandler http.Handler, leaseHandler http.Handler) http.Handler {
	mh := &peerMembersHandler{
		cluster: cluster,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", http.NotFound)
	mux.Handle(rafthttp.RaftPrefix, raftHandler)
	mux.Handle(rafthttp.RaftPrefix+"/", raftHandler)
	mux.Handle(peerMembersPrefix, mh)
	//mux.Handle(peerLearnersPrefix, mh)
	if leaseHandler != nil {
		mux.Handle(leasehttp.LeasePrefix, leaseHandler)
		mux.Handle(leasehttp.LeaseInternalPrefix, leaseHandler)
	}
	mux.HandleFunc(versionPath, versionHandler(cluster, serveVersion))
	return mux
}

type peerMembersHandler struct {
	cluster api.Cluster
}
type MembersOrlearners struct {
	Membs []*membership.Member `json:"members"`
	Learns []*membership.Learner `json:"learners"`
} 

func (h *peerMembersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, "GET") {
		return
	}
	w.Header().Set("X-Etcd-Cluster-ID", h.cluster.ID().String())

	if r.URL.Path != peerMembersPrefix /*|| r.URL.Path != peerLearnersPrefix*/{
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	ms := h.cluster.Members()
	ls := h.cluster.Learners()

	var lm MembersOrlearners
	lm.Membs = ms
	lm.Learns = ls
	fmt.Print("cluster members at peer.go are",lm.Membs,"\n")
	fmt.Print("cluster learners at peer.go are",lm.Learns,"\n")
	w.Header().Set("Content-Type", "application/json")
	hh := json.NewEncoder(w)
	fmt.Print("lm is ",lm,"\n")
	if err := hh.Encode(lm); err != nil {
		plog.Warningf("failed to encode members response (%v)", err)
	}
}
