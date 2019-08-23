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

package command

import (
	
	"strconv" 
	"github.com/spf13/cobra"
)




func NewConfigurationCommand() *cobra.Command {
	rc := &cobra.Command{
		Use:   "reconfiguration <subcommand>",
		Short: "Reconfiguration related commands",
		
		Run: newconfCommandFunc,
	}
	rc.Flags().StringVar(&memberPeerURLs, "peer-urls", "", "comma separated peer URLs .")
	return rc
}



func newconfCommandFunc(cmd *cobra.Command, args []string) {
	
	var confids []uint64
	
	for _, id := range args {
		confid, err := strconv.ParseUint(id, 16, 64)
		if err != nil{
			ExitWithError(ExitError, err)		
		}
		confids = append(confids, confid)
	}
	ctx, cancel := commandCtx(cmd)
	cli := mustClientFromCmd(cmd)
	resp, err := cli.Reconfiguration(ctx, confids)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	display.Reconfiguration(*resp)

}
