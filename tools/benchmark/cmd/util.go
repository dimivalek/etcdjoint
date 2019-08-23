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

package cmd

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/report"
	"google.golang.org/grpc/grpclog"
	"github.com/bgentry/speakeasy"
)

var (
	// dialTotal counts the number of mustCreateConn calls so that endpoint
	// connections can be handed out in round-robin order
	dialTotal int

	// leaderEps is a cache for holding endpoints of a leader node
	LeaderEps []string
	LeaderEpsnew []string
	// cache the username and password for multiple connections
	globalUserName string
	globalPassword string
)
/*type leader struct {
	// leaderEps is a cache for holding endpoints of a leader node
	leaderEps []string
	leaderEpsnew []string
}*/
func mustFindLeaderEndpoints(c *clientv3.Client) {
	resp, lerr := c.MemberList(context.TODO())
	if lerr != nil {
		fmt.Fprintf(os.Stderr, "failed to get a member list: %s\n", lerr)
		os.Exit(1)
	}

	leaderId := uint64(0)
	for _, ep := range c.Endpoints() {
		if sresp, serr := c.Status(context.TODO(), ep); serr == nil {
			leaderId = sresp.Leader
			break
		}
	}

	for _, m := range resp.Members {
		if m.ID == leaderId {
			//l := &leader{
			LeaderEps = m.ClientURLs
			//}
			fmt.Print("leaderEps",LeaderEps,"\n")
			return
		}
	}
	/*for _, l := range resp.Learners {
		if l.ID == leaderId {
			leaderEps = l.ClientURLs
			return
		}
	}*/
	fmt.Fprintf(os.Stderr, "failed to find a leader endpoint\n")
	os.Exit(1)
}

func LeaderEndpointchanged(client *clientv3.Client, leaderendpoint string) bool{
	//client := make([]*clientv3.Client, 1)
	fmt.Print("leader endpointsss changedddddddd \n")
	resp, lerr := client.MemberList(context.TODO())
	if lerr != nil {
		fmt.Fprintf(os.Stderr, "failed to get a member list: %s\n", lerr)
		os.Exit(1)
	}
	leaderId := uint64(0)
	syerr := client.Sync(context.TODO())
	if syerr != nil {
		fmt.Fprintf(os.Stderr, "failed to sync a member list: %s\n", syerr)
		os.Exit(1)
	}
	for _, ep := range client.Endpoints() {
		fmt.Print("ep endpoint",ep,"\n")
		if sresp, serr := client.Status(context.TODO(), ep); serr == nil {
			leaderId = sresp.Leader
			break
		}
	}
	for _, m := range resp.Members {
		fmt.Print("m.ID,m.ClientURLs",m.ID,m.ClientURLs,"\n")
		if m.ID == leaderId {
			//l := &leader{
			LeaderEpsnew = m.ClientURLs
			
			fmt.Print("leaderendpoint member ,LeaderEpsnew,LeaderEpsnew[0]",leaderendpoint,LeaderEpsnew[0],LeaderEpsnew,"\n")
			//}
			if LeaderEpsnew[0] != leaderendpoint {
				fmt.Print("LeaderEpsnew,LeaderEpsnew[0]",LeaderEpsnew[0],LeaderEpsnew,"\n")
				return true 		
			} 
		}
		
	}
	for _, l := range resp.Learners {
		fmt.Print("learner id \n")
		if l.ID == leaderId {
			//l := &leader{
			LeaderEpsnew = l.ClientURLs
			fmt.Print("leaderendpoint learner,LeaderEpsnew,LeaderEpsnew[0]",leaderendpoint,LeaderEpsnew[0],LeaderEpsnew,"\n")
			//}
			if LeaderEpsnew[0] != leaderendpoint {
				fmt.Print("LeaderEpsnew,LeaderEpsnew[0]",LeaderEpsnew[0],LeaderEpsnew,"\n")
				return true 		
			} 
		}
		
	}
	//fmt.Fprintf(os.Stderr, "failed to find a leader endpoint 2\n")
	//os.Exit(1)
	
	return LeaderEndpointchanged(client, leaderendpoint)

}

func getUsernamePassword(usernameFlag string) (string, string, error) {
	if globalUserName != "" && globalPassword != "" {
		return globalUserName, globalPassword, nil
	}
	colon := strings.Index(usernameFlag, ":")
	if colon == -1 {
		// Prompt for the password.
		password, err := speakeasy.Ask("Password: ")
		if err != nil {
			return "", "", err
		}
		globalUserName = usernameFlag
		globalPassword = password
	} else {
		globalUserName = usernameFlag[:colon]
		globalPassword = usernameFlag[colon+1:]
	}
	return globalUserName, globalPassword, nil
}

func mustCreateConn1() (*clientv3.Client,[]string) {
	connEndpoints := LeaderEpsnew
	if len(connEndpoints) == 0 {
		connEndpoints = LeaderEps
	}

	fmt.Print("connEndpoints",connEndpoints,"\n")
	if len(connEndpoints) == 0 {
		connEndpoints = []string{endpoints[dialTotal%len(endpoints)]}
		dialTotal++
	}
	fmt.Print("connEndpoints",connEndpoints,"\n")
	cfg := clientv3.Config{
		Endpoints:   connEndpoints,
		DialTimeout: dialTimeout,
	}
	if !tls.Empty() {
		cfgtls, err := tls.ClientConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad tls config: %v\n", err)
			os.Exit(1)
		}
		cfg.TLS = cfgtls
	}

	if len(user) != 0 {
		username, password, err := getUsernamePassword(user)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad user information: %s %v\n", user, err)
			os.Exit(1)
		}
		cfg.Username = username
		cfg.Password = password

	}

	client, err := clientv3.New(cfg)
	if targetLeader && len(LeaderEps) == 0 {
		mustFindLeaderEndpoints(client)
		client.Close()
		return mustCreateConn1()
	}

	clientv3.SetLogger(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))

	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}

	return client,connEndpoints
}
func mustCreateConn() *clientv3.Client {
	connEndpoints := LeaderEps

	fmt.Print("connEndpoints",connEndpoints,"\n")
	if len(connEndpoints) == 0 {
		connEndpoints = []string{endpoints[dialTotal%len(endpoints)]}
		dialTotal++
	}
	fmt.Print("connEndpoints",connEndpoints,"\n")
	cfg := clientv3.Config{
		Endpoints:   connEndpoints,
		DialTimeout: dialTimeout,
	}
	if !tls.Empty() {
		cfgtls, err := tls.ClientConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad tls config: %v\n", err)
			os.Exit(1)
		}
		cfg.TLS = cfgtls
	}

	if len(user) != 0 {
		username, password, err := getUsernamePassword(user)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad user information: %s %v\n", user, err)
			os.Exit(1)
		}
		cfg.Username = username
		cfg.Password = password

	}

	client, err := clientv3.New(cfg)
	if targetLeader && len(LeaderEps) == 0 {
		mustFindLeaderEndpoints(client)
		client.Close()
		return mustCreateConn()
	}

	clientv3.SetLogger(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))

	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}

	return client
}

func mustCreateClients(totalClients, totalConns uint) []*clientv3.Client {
	conns := make([]*clientv3.Client, totalConns)
	
	for i := range conns {
		conns[i] = mustCreateConn()
	}

	clients := make([]*clientv3.Client, totalClients)
	for i := range clients {
		clients[i] = conns[i%int(totalConns)]
	}
	return clients
}

func mustCreateClients1(totalClients, totalConns uint) ([]*clientv3.Client,[]string) {
	conns := make([]*clientv3.Client, totalConns)
	leaderendpoint := make([]string, totalConns)
	for i := range conns {
		conns[i],leaderendpoint = mustCreateConn1()
	}

	clients := make([]*clientv3.Client, totalClients)
	for i := range clients {
		clients[i] = conns[i%int(totalConns)]
	}
	return clients,leaderendpoint
}
func mayRecreateClients(clients *clientv3.Client, leaderendpoint string) bool {
	changed := LeaderEndpointchanged(clients,leaderendpoint)
	return changed
}
func mustRandBytes(n int) []byte {
	rb := make([]byte, n)
	_, err := rand.Read(rb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate value: %v\n", err)
		os.Exit(1)
	}
	return rb
}

func newReport() report.Report {
	p := "%4.4f"
	if precise {
		p = "%g"
	}
	if sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}

func newWeightedReport() report.Report {
	p := "%4.4f"
	if precise {
		p = "%g"
	}
	if sample {
		return report.NewReportSample(p)
	}
	return report.NewWeightedReport(report.NewReport(p), p)
}
