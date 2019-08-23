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
	"fmt"
	"strings"
	"strconv"
	"github.com/spf13/cobra"
)

var learnerPeerURLs string

func NewLearnerCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "learner <subcommand>",
		Short: "Membership related commands",
	}
	
	mc.AddCommand(NewLearnerAddCommand())
	mc.AddCommand(NewLearnerRemoveCommand())
	return mc
}

// NewLearnerAddCommand returns the cobra command for "learner add".
func NewLearnerAddCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "add <learnerName> [options]",
		Short: "Adds a learner into the wider cluster",

		Run: learnerAddCommandFunc,
	}

	cc.Flags().StringVar(&memberPeerURLs, "peer-urls", "", "comma separated peer URLs for the new learner.")

	return cc
}

// NewMemberRemoveCommand returns the cobra command for "member remove".
func NewLearnerRemoveCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "remove <memberID>",
		Short: "Removes a learner from the cluster",

		Run: learnerRemoveCommandFunc,
	}

	return cc
}
// learnerAddCommandFunc executes the "learner add" command.
func learnerAddCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("learner name not provided."))
	}
	if len(memberPeerURLs) == 0 {
		ExitWithError(ExitBadArgs, fmt.Errorf("member peer urls not provided."))
	}

	urls := strings.Split(memberPeerURLs, ",")
	ctx, cancel := commandCtx(cmd)
	cli := mustClientFromCmd(cmd)
	resp, err := cli.LearnerAdd(ctx, urls)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}

	display.LearnerAdd(*resp)

}

func learnerRemoveCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("learner ID is not provided"))
	}

	id, err := strconv.ParseUint(args[0], 16, 64)
	if err != nil {
		ExitWithError(ExitBadArgs, fmt.Errorf("bad learner ID arg (%v), expecting ID in Hex", err))
	}

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).LearnerRemove(ctx, id)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	display.LearnerRemove(id, *resp)
}
