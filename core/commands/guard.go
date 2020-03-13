package commands

import (
	"fmt"
	"strings"
	"time"

	cmds "github.com/TRON-US/go-btfs-cmds"
	"github.com/TRON-US/go-btfs/core/commands/cmdenv"
	"github.com/TRON-US/go-btfs/core/commands/storage"
	"github.com/TRON-US/go-btfs/core/guard"
	"github.com/TRON-US/go-btfs/core/hub"
	cconfig "github.com/tron-us/go-btfs-common/config"

	"github.com/ipfs/go-cid"
)

const (
	guardUrlOptionName                   = "url"
	guardQuestionCountPerShardOptionName = "questions-per-shard"
	guardHostsOptionName                 = "hosts"
)

var GuardCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Interact with grd services from BTFS client.",
		ShortDescription: `
Connect with grd functions directly through this command.
The subcommands here are mostly for debugging and testing purposes.`,
	},
	Subcommands: map[string]*cmds.Command{
		"test": guardTestCmd,
	},
}

var guardTestCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Send tests to grd service endpoints from BTFS client.",
		ShortDescription: `
This command contains subcommands that are typically for development purposes
by letting the BTFS client test individual grd endpoints.`,
	},
	Subcommands: map[string]*cmds.Command{
		"send-challenges": guardSendChallengesCmd,
	},
}

var guardSendChallengesCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Send shard challenge questions from BTFS client.",
		ShortDescription: `
Sends all shard challenge questions under a reed-solomon encoded file
to the grd service.`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("file-hash", true, false, "File hash to generate the questions from.").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.StringOption(guardUrlOptionName, "u", "Guard service url including protocol and port. Default: reads from BTFS config."),
		cmds.IntOption(guardQuestionCountPerShardOptionName, "q", "Number of challenge questions per shard to generate").WithDefault(cconfig.ConstMinQuestionsCountPerChallenge),
		cmds.StringOption(guardHostsOptionName, "sh", "List of hosts for each shard, ordered sequentially and separated by ','. Default: reads from BTFS ds."),
	},
	RunTimeout: 30 * time.Second,
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		// get config settings
		cfg, err := cmdenv.GetConfig(env)
		if err != nil {
			return err
		}
		if !cfg.Experimental.StorageClientEnabled {
			return fmt.Errorf("storage client api not enabled")
		}
		// get node
		n, err := cmdenv.GetNode(env)
		if err != nil {
			return err
		}
		// get core api
		api, err := cmdenv.GetApi(env, req)
		if err != nil {
			return err
		}
		rootHash, err := cid.Parse(req.Arguments[0])
		if err != nil {
			return err
		}
		shardHashes, _, err := storage.CheckAndGetReedSolomonShardHashes(req.Context, n, api, rootHash)
		if err != nil {
			return err
		}
		var hostIDs []string
		if hl, found := req.Options[guardHostsOptionName].(string); found {
			hostIDs = strings.Split(hl, ",")
		} else {
			hosts, err := storage.GetHostsFromDatastore(req.Context, n, hub.HubModeAll, len(shardHashes))
			if err != nil {
				return err
			}
			for _, ni := range hosts {
				hostIDs = append(hostIDs, ni.NodeId)
			}
		}

		qCount, _ := req.Options[guardQuestionCountPerShardOptionName].(int)
		questions, err := guard.PrepCustomFileChallengeQuestions(req.Context, n, api, nil,
			rootHash, shardHashes, hostIDs, qCount)
		if err != nil {
			return err
		}
		// check if we need to update config for a different grd url
		if gu, found := req.Options[guardUrlOptionName].(string); found {
			cfg, err = cfg.Clone()
			if err != nil {
				return err
			}
			cfg.Services.GuardDomain = gu
		}
		// send to grd
		return guard.SendChallengeQuestions(req.Context, cfg, rootHash, questions)
	},
}
