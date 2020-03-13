package upload

import (
	"context"
	"errors"
	"fmt"
	shardpb "github.com/TRON-US/go-btfs/core/commands/store/upload/pb/shard"
	"strconv"
	"time"

	"github.com/TRON-US/go-btfs/core/commands/cmdenv"
	"github.com/TRON-US/go-btfs/core/commands/storage"
	"github.com/TRON-US/go-btfs/core/commands/store/upload/ds"
	"github.com/TRON-US/go-btfs/core/commands/store/upload/hosts"
	"github.com/TRON-US/go-btfs/core/corehttp/remote"
	"github.com/TRON-US/go-btfs/core/escrow"
	"github.com/TRON-US/go-btfs/core/guard"
	"github.com/TRON-US/go-btfs/core/hub"

	cmds "github.com/TRON-US/go-btfs-cmds"
	coreiface "github.com/TRON-US/interface-go-btfs-core"
	"github.com/TRON-US/interface-go-btfs-core/path"

	"github.com/alecthomas/units"
	"github.com/google/uuid"
	cidlib "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	uploadPriceOptionName            = "price"
	replicationFactorOptionName      = "replication-factor"
	hostSelectModeOptionName         = "host-select-mode"
	hostSelectionOptionName          = "host-selection"
	testOnlyOptionName               = "host-search-local"
	storageLengthOptionName          = "storage-length"
	customizedPayoutOptionName       = "customize-payout"
	customizedPayoutPeriodOptionName = "customize-payout-period"

	defaultRepFactor     = 3
	defaultStorageLength = 30
)

var StorageUploadCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Store files on BTFS network nodes through BTT payment.",
		ShortDescription: `
By default, BTFS selects hosts based on overall score according to the current client's environment.
To upload a file, <file-hash> must refer to a reed-solomon encoded file.

To create a reed-solomon encoded file from a normal file:

    $ btfs add --chunker=reed-solomon <file>
    added <file-hash> <file>

Run command to upload:

    $ btfs storage upload <file-hash>

To custom upload and store a file on specific hosts:
    Use -m with 'custom' mode, and put host identifiers in -s, with multiple hosts separated by ','.

    # Upload a file to a set of hosts
    # Total # of hosts (N) must match # of shards in the first DAG level of root file hash
    $ btfs storage upload <file-hash> -m=custom -s=<host1-peer-id>,<host2-peer-id>,...,<hostN-peer-id>

    # Upload specific shards to a set of hosts
    # Total # of hosts (N) must match # of shards given
    $ btfs storage upload <shard-hash1> <shard-hash2> ... <shard-hashN> -l -m=custom -s=<host1-peer-id>,<host2-peer-id>,...,<hostN-peer-id>

Use status command to check for completion:
    $ btfs storage upload status <session-id> | jq`,
	},
	Subcommands: map[string]*cmds.Command{
		//"init":              storageUploadInitCmd,
		"recvcontract": StorageUploadRecvContractCmd,
		//"status":            storageUploadStatusCmd,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("file-hash", true, false, "Hash of file to upload."),
	},
	Options: []cmds.Option{
		cmds.Int64Option(uploadPriceOptionName, "p", "Max price per GiB per day of storage in JUST."),
		cmds.IntOption(replicationFactorOptionName, "r", "Replication factor for the file with erasure coding built-in.").WithDefault(defaultRepFactor),
		cmds.StringOption(hostSelectModeOptionName, "m", "Based on this mode to select hosts and upload automatically. Default: mode set in config option Experimental.HostsSyncMode."),
		cmds.StringOption(hostSelectionOptionName, "s", "Use only these selected hosts in order on 'custom' mode. Use ',' as delimiter."),
		cmds.BoolOption(testOnlyOptionName, "t", "Enable host search under all domains 0.0.0.0 (useful for local test)."),
		cmds.IntOption(storageLengthOptionName, "len", "File storage period on hosts in days.").WithDefault(defaultStorageLength),
		cmds.BoolOption(customizedPayoutOptionName, "Enable file storage customized payout schedule.").WithDefault(false),
		cmds.IntOption(customizedPayoutPeriodOptionName, "Period of customized payout schedule.").WithDefault(1),
	},
	RunTimeout: 15 * time.Minute,
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {

		n, err := cmdenv.GetNode(env)
		if err != nil {
			return err
		}

		cfg, err := cmdenv.GetConfig(env)
		if err != nil {
			return err
		}

		api, err := cmdenv.GetApi(env, req)
		if err != nil {
			return err
		}

		if len(req.Arguments) != 1 {
			return fmt.Errorf("need one and only one root file hash")
		}
		fileHash := req.Arguments[0]
		fileCid, err := cidlib.Parse(fileHash)
		if err != nil {
			return err
		}

		cids, fileSize, err := storage.CheckAndGetReedSolomonShardHashes(req.Context, n, api, fileCid)
		if err != nil || len(cids) == 0 {
			return fmt.Errorf("invalid hash: %s", err)
		}
		fmt.Println("fileSize", fileSize)

		shardHashes := make([]string, 0)
		for _, c := range cids {
			shardHashes = append(shardHashes, c.String())
		}

		hp := hosts.GetHostProvider(req.Context, n, cfg.Experimental.HostsSyncMode, api)

		ns, err := hub.GetSettings(req.Context, cfg.Services.HubDomain,
			n.Identity.String(), n.Repo.Datastore())
		if err != nil {
			return err
		}

		storageLength := req.Options[storageLengthOptionName].(int)
		if uint64(storageLength) < ns.StorageTimeMin {
			return fmt.Errorf("invalid storage len. want: >= %d, got: %d",
				ns.StorageTimeMin, storageLength)
		}

		price, found := req.Options[uploadPriceOptionName].(int64)
		if !found {
			price = int64(ns.StoragePriceAsk)
		}

		shardSize, err := getContractSizeFromCid(req.Context, cids[0], api)
		if err != nil {
			return err
		}

		ss, err := ds.GetSession("", n.Identity.String(), &ds.SessionInitParams{
			Ctx:         req.Context,
			Cfg:         cfg,
			Ds:          n.Repo.Datastore(),
			N:           n,
			Api:         api,
			RenterId:    n.Identity.String(),
			FileHash:    fileHash,
			ShardHashes: shardHashes,
		})
		if err != nil {
			return err
		}

		for i, h := range shardHashes {
			go func(i int, h string) error {
				host, err := hp.NextValidHost()
				if err != nil {
					return err
				}
				totalPay := int64(float64(shardSize) / float64(units.GiB) * float64(price) * float64(storageLength))
				hostPid, err := peer.IDB58Decode(host)
				if err != nil {
					return err
				}
				contract, err := escrow.NewContract(cfg, uuid.New().String(), n, hostPid, totalPay, false, 0, "")
				if err != nil {
					return err
				}
				halfSignedEscrowContract, err := escrow.SignContractAndMarshal(contract, nil, n.PrivateKey, true)
				if err != nil {
					return fmt.Errorf("sign escrow contract and maorshal failed: [%v] ", err)
				}

				shard, err := ds.GetShard(req.Context, n.Repo.Datastore(), n.Identity.String(), ss.Id, h)
				if err != nil {
					return err
				}
				md, err := shard.Metadata()
				if err != nil {
					return err
				}
				guardContractMeta, err := NewContract(md, h, cfg, n.Identity.String())
				if err != nil {
					return err
				}
				halfSignedGuardContract, err := guard.SignedContractAndMarshal(guardContractMeta, nil, nil,
					n.PrivateKey, true, false, n.Identity.Pretty(), n.Identity.Pretty())
				if err != nil {
					return fmt.Errorf("fail to sign grd contract and marshal: [%v] ", err)
				}
				_, err = remote.P2PCall(req.Context, n, hostPid, "/storage/upload/init",
					ss.Id,
					fileHash,
					h,
					strconv.FormatInt(price, 10),
					halfSignedEscrowContract,
					halfSignedGuardContract,
					strconv.FormatInt(int64(storageLength), 10),
					strconv.FormatInt(int64(shardSize), 10),
					strconv.Itoa(i),
				)
				if err != nil {
					return err
				}
				shard.Contract(&shardpb.Contracts{
					HalfSignedEscrowContract: halfSignedEscrowContract,
					HalfSignedGuardContract:  halfSignedGuardContract,
				})
				return err
			}(i, h)
		}

		go func(f *ds.Session, ctx context.Context, numShards int) {
			tick := time.Tick(5 * time.Second)
			for true {
				select {
				case <-tick:
					completeNum, errorNum, err := f.GetCompleteShardsNum()
					if err != nil {
						continue
					}
					if completeNum == numShards {
						f.Submit()
						return
					} else if errorNum > 0 {
						f.Error(errors.New("there are error shards"))
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(ss, ss.Ctx, len(shardHashes))

		seRes := &UploadRes{
			ID: ss.Id,
		}
		return res.Emit(seRes)
	},
	Type: UploadRes{},
}

type UploadRes struct {
	ID string
}

func getContractSizeFromCid(ctx context.Context, hash cidlib.Cid, api coreiface.CoreAPI) (uint64, error) {
	leafPath := path.IpfsPath(hash)
	ipldNode, err := api.ResolveNode(ctx, leafPath)
	if err != nil {
		return 0, err
	}
	return ipldNode.Size()
}
