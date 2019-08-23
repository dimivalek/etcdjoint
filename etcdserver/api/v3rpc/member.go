// Copyright 2016 The etcd Authors
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

package v3rpc

import (
	"context"
	"time"
	"fmt"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/pkg/types"
)

type ClusterServer struct {
	cluster api.Cluster
	server  etcdserver.ServerV3
}

func NewClusterServer(s etcdserver.ServerV3) *ClusterServer {
	return &ClusterServer{
		cluster: s.Cluster(),
		server:  s,
	}
}

func (cs *ClusterServer) MemberAdd(ctx context.Context, r *pb.MemberAddRequest) (*pb.MemberAddResponse, error) {
	urls, err := types.NewURLs(r.PeerURLs)
	if err != nil {
		return nil, rpctypes.ErrGRPCMemberBadURLs
	}

	now := time.Now()
	m := membership.NewMember("", urls, "", &now)
	membs, merr := cs.server.AddMember(ctx, *m)
	if merr != nil {
		return nil, togRPCError(merr)
	}

	return &pb.MemberAddResponse{
		Header:  cs.header(),
		Member:  &pb.Member{ID: uint64(m.ID), PeerURLs: m.PeerURLs},
		Members: membersToProtoMembers(membs),
	}, nil
}

func (cs *ClusterServer) LearnerAdd(ctx context.Context, r *pb.LearnerAddRequest) (*pb.LearnerAddResponse, error) {
	fmt.Print("r.urls are :",r.PeerURLs,"\n")
	urls, err := types.NewURLs(r.PeerURLs)
	if err != nil {
		return nil, rpctypes.ErrGRPCMemberBadURLs
	}
	fmt.Print(" learner add etcdserver/api/v3rpc/member.go \n")
	now := time.Now()
	////is actually learner not member
	m := membership.NewLearner("", urls, "", &now)
	learns, merr := cs.server.AddLearner(ctx, *m)
	//learnerToProtoLearner(learn)
	if merr != nil {
		return nil, togRPCError(merr)
	}

	return &pb.LearnerAddResponse{
		Header:  cs.header(),
		Learner:/*learnerToProtoLearner(learn)*/ &pb.Learner{ID: uint64(m.ID), PeerURLs: m.PeerURLs},
		Learners: learnersToProtoLearners(learns),
	}, nil
}

func (cs *ClusterServer) Reconfiguration(ctx context.Context, r *pb.ReconfigurationRequest) (*pb.ReconfigurationResponse, error) {
	confids:= r.ConfIDs
	
	fmt.Print(" confids are ",confids," \n")
	//now := time.Now()
	reconf, merr := cs.server.Reconfiguration(ctx, confids)
	fmt.Print(" reconf \n")
	if merr != nil {
		return nil, togRPCError(merr)
	}
	if reconf != nil {
			
	}
	return &pb.ReconfigurationResponse{
		Header:  cs.header(),
		ConfIDs:confids,

	}, nil
}
func (cs *ClusterServer) MemberRemove(ctx context.Context, r *pb.MemberRemoveRequest) (*pb.MemberRemoveResponse, error) {
	membs, err := cs.server.RemoveMember(ctx, r.ID)
	if err != nil {
		return nil, togRPCError(err)
	}
	return &pb.MemberRemoveResponse{Header: cs.header(), Members: membersToProtoMembers(membs)}, nil
}

func (cs *ClusterServer) LearnerRemove(ctx context.Context, r *pb.LearnerRemoveRequest) (*pb.LearnerRemoveResponse, error) {
	learns, err := cs.server.RemoveLearner(ctx, r.ID)
	if err != nil {
		return nil, togRPCError(err)
	}
	return &pb.LearnerRemoveResponse{Header: cs.header(), Learners: learnersToProtoLearners(learns)}, nil
}
func (cs *ClusterServer) MemberUpdate(ctx context.Context, r *pb.MemberUpdateRequest) (*pb.MemberUpdateResponse, error) {
	m := membership.Member{
		ID:             types.ID(r.ID),
		RaftAttributes: membership.RaftAttributes{PeerURLs: r.PeerURLs},
	}
	membs, err := cs.server.UpdateMember(ctx, m)
	if err != nil {
		return nil, togRPCError(err)
	}
	return &pb.MemberUpdateResponse{Header: cs.header(), Members: membersToProtoMembers(membs)}, nil
}

func (cs *ClusterServer) MemberList(ctx context.Context, r *pb.MemberListRequest) (*pb.MemberListResponse, error) {
	membs := membersToProtoMembers(cs.cluster.Members())
	learns := learnersToProtoLearners(cs.cluster.Learners())
	return &pb.MemberListResponse{Header: cs.header(), Members: membs, Learners: learns}, nil
}

func (cs *ClusterServer) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: uint64(cs.cluster.ID()), MemberId: uint64(cs.server.ID()), RaftTerm: cs.server.Term()}
}

func membersToProtoMembers(membs []*membership.Member) []*pb.Member {
	protoMembs := make([]*pb.Member, len(membs))
	for i := range membs {
		protoMembs[i] = &pb.Member{
			Name:       membs[i].Name,
			ID:         uint64(membs[i].ID),
			PeerURLs:   membs[i].PeerURLs,
			ClientURLs: membs[i].ClientURLs,
		}
	}
	return protoMembs
}

func learnersToProtoLearners(learns []*membership.Learner) []*pb.Learner {
	//var startedLearners uint64
	/*if len(learns)==0 {
		protoLearns := make([]*pb.Learner, 1)
		for i := range learns {
			protoLearns[i] = &pb.Learner{
				Name:       learns[i].Name,
				ID:         uint64(learns[i].ID),
				PeerURLs:   learns[i].PeerURLs,
				ClientURLs: learns[i].ClientURLs,
			}
		}
		return protoLearns
	} else {*/
		protoLearns := make([]*pb.Learner, len(learns))
		fmt.Print("protolearns length is",len(learns),"\n")
		for i := range learns {
			protoLearns[i] = &pb.Learner{
				Name:       learns[i].Name,
				ID:         uint64(learns[i].ID),
				PeerURLs:   learns[i].PeerURLs,
				ClientURLs: learns[i].ClientURLs,
			}
		}
		return protoLearns
	//}
	//protoLearns := make([]*pb.Learner, len(learns))
	
}
func learnerToProtoLearner(learn *membership.Learner) *pb.Learner {
		protoLearn := &pb.Learner{
			Name:       learn.Name,
			ID:         uint64(learn.ID),
			PeerURLs:   learn.PeerURLs,
			ClientURLs: learn.ClientURLs,
		}
	return protoLearn
}
