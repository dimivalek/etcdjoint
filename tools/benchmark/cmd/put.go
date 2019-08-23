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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/report"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
)

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Benchmark put",

	Run: putFunc,
}

var (
	keySize int
	valSize int

	putTotal int
	putRate  int

	keySpaceSize int
	seqKeys      bool

	compactInterval   time.Duration
	compactIndexDelta int64
)

func init() {
	RootCmd.AddCommand(putCmd)
	putCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of put request")
	putCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of put request")
	putCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")

	putCmd.Flags().IntVar(&putTotal, "total", 10000, "Total number of put requests")
	putCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
	putCmd.Flags().BoolVar(&seqKeys, "sequential-keys", false, "Use sequential keys")
	putCmd.Flags().DurationVar(&compactInterval, "compact-interval", 0, `Interval to compact database (do not duplicate this with etcd's 'auto-compaction-retention' flag) (e.g. --compact-interval=5m compacts every 5-minute)`)
	putCmd.Flags().Int64Var(&compactIndexDelta, "compact-index-delta", 1000, "Delta between current revision and compact revision (e.g. current revision 10000, compact at 9000)")
}

func putFunc(cmd *cobra.Command, args []string) {
	var elapsed1,elapsed2 time.Duration
	var st1 time.Time
	var seconds1, seconds2 float64
	var checkchannel int
	//var leftrequests int
	//start := make(chan struct{})
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", keySpaceSize)
		os.Exit(1)
	}
	req := make(chan v3.Op, totalClients)
	requests := make(chan v3.Op, totalClients)
	requests2 := make(chan v3.Op, totalClients)
	requests3 := make(chan v3.Op, totalClients)
	requests4 := make(chan v3.Op, totalClients)
	checkchannel = 0
	if putRate == 0 {
		putRate = math.MaxInt32
	}
	limit := rate.NewLimiter(rate.Limit(putRate), 1)
	clients,leaderendpoint := mustCreateClients1(totalClients, totalConns)
	k, v := make([]byte, keySize), string(mustRandBytes(valSize))

	bar = pb.New(putTotal)
	bar.Format("Bom !")
	bar.Start()
	//start := make(chan struct{})
	r := newReport()
	for i := range clients {
		//fmt.Print("leaderendpoint[0]",leaderendpoint[0],"\n")
		/*if mayRecreateClients(cl,leaderendpoint[0]) == true {
			clients,leaderendpoint = mustCreateClients1(totalClients-uint(i), totalConns)		
		}*/
		wg.Add(1)
		go func(c *v3.Client) {
			defer wg.Done()
			for op := range requests {
				//leftrequests = requests - i
				limit.Wait(context.Background())
				st := time.Now()
				_, err := c.Do(context.Background(), op)
				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
				bar.Increment()
			}
		}(clients[i])
	}

	go func() {
		
		for i := 0; i < putTotal; i++ {
			st1 = time.Now()
			elapsed1 = time.Since(st1)
			seconds1 = elapsed1.Seconds()
			if seqKeys {
				//fmt.Print("seqkeys",seqKeys)
				binary.PutVarint(k, int64(i%keySpaceSize))
			} else {
				//fmt.Print("seqkeys",seqKeys)
				binary.PutVarint(k, int64(rand.Intn(keySpaceSize)))
			}
			if checkchannel == 0 {
				requests <- v3.OpPut(string(k), v)
			}else if checkchannel == 1 {
				requests2 <- v3.OpPut(string(k), v)
			}else if checkchannel == 2 {
				requests3 <- v3.OpPut(string(k), v)
			}else if checkchannel == 3 {
				requests4 <- v3.OpPut(string(k), v)
			}
			//req = requests//////////////
			elapsed2 = time.Since(st1)	
			seconds2 = elapsed2.Seconds()
			if seconds2 - seconds1 >= 1.5 {
				changed := mayRecreateClients(clients[0],leaderendpoint[0])
				//call function that starts new channel
				if changed == true {
					clients,leaderendpoint = mustCreateClients1(totalClients, totalConns)
					checkchannel = checkchannel +1
					//checkchannel = true
					//wg.Wait()
					if checkchannel == 0 {
						//requests <- v3.OpPut(string(k), v)
						req = requests
					}else if checkchannel == 1 {
						//requests2 <- v3.OpPut(string(k), v)
						req = requests2
						close(requests)
					}else if checkchannel == 2 {
						//requests3 <- v3.OpPut(string(k), v)
						req = requests3
						close(requests2)
					}else if checkchannel == 3 {
						//requests4 <- v3.OpPut(string(k), v)
						req = requests4
						close(requests3)
					}
					//close(requests)
					//close(start)
					for i := range clients {
						fmt.Print("leaderendpoint[0]",leaderendpoint[0],"\n")
						/*if mayRecreateClients(cl,leaderendpoint[0]) == true {
							clients,leaderendpoint = mustCreateClients1(totalClients-uint(i), totalConns)		
						}*/
						wg.Add(1)
						go func(c *v3.Client) {
							defer wg.Done()
							for op := range req {
								limit.Wait(context.Background())

								st := time.Now()
								_, err := c.Do(context.Background(), op)
								r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
								bar.Increment()
							}
						}(clients[i])
					}
				}
			}
			
		}
		//if checkchannel == false {
			//close(requests)
		//}
		close(req)
	}()

	if compactInterval > 0 {
		go func() {
			for {
				time.Sleep(compactInterval)
				compactKV(clients)
			}
		}()
	}
	rc :=  r.Run()
	wg.Wait()
	//wg1.Wait()
	close(r.Results())
	bar.Finish()
	fmt.Println(<-rc)
}

func compactKV(clients []*v3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := clients[0].KV.Get(ctx, "foo")
	cancel()
	if err != nil {
		panic(err)
	}
	revToCompact := max(0, resp.Header.Revision-compactIndexDelta)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = clients[0].KV.Compact(ctx, revToCompact)
	cancel()
	if err != nil {
		panic(err)
	}
}

func max(n1, n2 int64) int64 {
	if n1 > n2 {
		return n1
	}
	return n2
}
