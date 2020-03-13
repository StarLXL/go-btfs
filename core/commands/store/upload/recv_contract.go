package upload

import (
	cmds "github.com/TRON-US/go-btfs-cmds"
	"github.com/TRON-US/go-btfs/core/commands/cmdenv"
	"github.com/TRON-US/go-btfs/core/commands/store/upload/ds"
	"github.com/TRON-US/go-btfs/core/guard"
	"github.com/prometheus/common/log"
)

var StorageUploadRecvContractCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "For renter client to receive half signed contracts.",
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("session-id", true, false, "Session ID which renter uses to store all shards information."),
		cmds.StringArg("shard-hash", true, false, "Shard the storage node should fetch."),
		cmds.StringArg("shard-index", true, false, "Index of shard within the encoding scheme."),
		cmds.StringArg("escrow-contract", true, false, "Signed Escrow contract."),
		cmds.StringArg("guard-contract", true, false, "Signed Guard contract."),
	},
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		log.Info("recv contract...")
		escrowContractBytes := []byte(req.Arguments[3])
		guardContractBytes := []byte(req.Arguments[4])
		ssID := req.Arguments[0]
		n, err := cmdenv.GetNode(env)
		if err != nil {
			return err
		}
		shardHash := req.Arguments[1]
		ss, err := ds.GetSession(ssID, n.Identity.Pretty(), nil)
		if err != nil {
			return err
		}
		s, err := ds.GetShard(ss.Ctx, n.Repo.Datastore(), n.Identity.String(), ssID, shardHash)
		if err != nil {
			return err
		}
		guardContract, err := guard.UnmarshalGuardContract(guardContractBytes)
		if err != nil {
			s.Error(err)
			return err
		}
		s.Complete(escrowContractBytes, guardContract)
		return nil
	},
}
