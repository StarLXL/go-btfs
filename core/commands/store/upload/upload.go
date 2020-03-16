package upload

import (
	"context"
	"errors"
	"fmt"
	config "github.com/TRON-US/go-btfs-config"
	"github.com/TRON-US/go-btfs/core"
	"github.com/TRON-US/go-btfs/core/commands/store/upload/helper"
	shardpb "github.com/TRON-US/go-btfs/core/commands/store/upload/pb/shard"
	"github.com/TRON-US/go-btfs/core/guard"
	"github.com/cenkalti/backoff/v3"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/prometheus/common/log"
	"github.com/tron-us/go-btfs-common/crypto"
	escrowpb "github.com/tron-us/go-btfs-common/protos/escrow"
	guardpb "github.com/tron-us/go-btfs-common/protos/guard"
	"github.com/tron-us/go-btfs-common/utils/grpc"
	"strconv"
	"time"

	"github.com/TRON-US/go-btfs/core/commands/cmdenv"
	"github.com/TRON-US/go-btfs/core/commands/storage"
	"github.com/TRON-US/go-btfs/core/commands/store/upload/ds"
	"github.com/TRON-US/go-btfs/core/commands/store/upload/hosts"
	"github.com/TRON-US/go-btfs/core/corehttp/remote"
	"github.com/TRON-US/go-btfs/core/escrow"
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

var (
	bo = func() *backoff.ExponentialBackOff {
		bo := backoff.NewExponentialBackOff()
		bo.InitialInterval = 10 * time.Second
		bo.MaxElapsedTime = 24 * time.Hour
		bo.Multiplier = 1.5
		bo.MaxInterval = 5 * time.Minute
		return bo
	}()
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
		"init":         StorageUploadInitCmd,
		"recvcontract": StorageUploadRecvContractCmd,
		"status":       storageUploadStatusCmd,
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
			go func(i int, h string) {
				host, err := hp.NextValidHost(price)
				if err != nil {
					ss.Error(err)
					return
				}
				totalPay := int64(float64(shardSize) / float64(units.GiB) * float64(price) * float64(storageLength))
				if totalPay <= 0 {
					totalPay = 1
				}
				md := &shardpb.Metadata{
					Index:          int32(i),
					SessionId:      ss.Id,
					FileHash:       fileHash,
					ShardFileSize:  int64(shardSize),
					StorageLength:  int64(storageLength),
					ContractId:     ss.Id,
					Receiver:       host,
					Price:          price,
					TotalPay:       totalPay,
					StartTime:      time.Now().UTC(),
					ContractLength: time.Duration(storageLength*24) * time.Hour,
				}
				shard, err := ds.GetShard(req.Context, n.Repo.Datastore(), n.Identity.String(), ss.Id, h)
				if err != nil {
					ss.Error(err)
					return
				}
				err = shard.Init(md)
				if err != nil {
					ss.Error(err)
					return
				}

				hostPid, err := peer.IDB58Decode(host)
				if err != nil {
					ss.Error(err)
					return
				}
				contract, err := escrow.NewContract(cfg, uuid.New().String(), n, hostPid, totalPay, false, 0, "")
				if err != nil {
					ss.Error(err)
					return
				}
				halfSignedEscrowContract, err := escrow.SignContractAndMarshal(contract, nil, n.PrivateKey, true)
				if err != nil {
					ss.Error(fmt.Errorf("sign escrow contract and maorshal failed: [%v] ", err))
					return
				}
				guardContractMeta, err := NewContract(md, h, cfg, n.Identity.String())
				if err != nil {
					ss.Error(err)
					return
				}
				halfSignedGuardContract, err := guard.SignedContractAndMarshal(guardContractMeta, nil, nil,
					n.PrivateKey, true, false, n.Identity.Pretty(), n.Identity.Pretty())
				if err != nil {
					ss.Error(fmt.Errorf("fail to sign grd contract and marshal: [%v] ", err))
					return
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
					ss.Error(err)
					return
				}
				shard.Contract(&shardpb.Contracts{
					HalfSignedEscrowContract: halfSignedEscrowContract,
					HalfSignedGuardContract:  halfSignedGuardContract,
				})
				return
			}(i, h)
		}

		go func(f *ds.Session, ctx context.Context, numShards int) {
			tick := time.Tick(5 * time.Second)
			for true {
				select {
				case <-tick:
					completeNum, errorNum, err := f.GetCompleteShardsNum()
					log.Info("completeNum:", completeNum, ",errorNum:", errorNum)
					if err != nil {
						continue
					}
					if completeNum == numShards {
						doSubmit(f)
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

func doSubmit(f *ds.Session) {
	f.Submit()
	bs, t, err := f.PrepareContractFromShard()
	if err != nil {
		f.Error(err)
		return
	}
	// check account balance, if not enough for the totalPrice do not submit to escrow
	balance, err := escrow.Balance(f.Ctx, f.Cfg)
	if err != nil {
		f.Error(err)
		return
	}
	if balance < t {
		f.Error(fmt.Errorf("not enough balance to submit contract, current balance is [%v]", balance))
		return
	}
	req, err := escrow.NewContractRequest(f.Cfg, bs, t)
	if err != nil {
		f.Error(err)
		return
	}
	var amount int64 = 0
	for _, c := range req.Contract {
		amount += c.Contract.Amount
	}
	submitContractRes, err := escrow.SubmitContractToEscrow(f.Ctx, f.Cfg, req)
	if err != nil {
		f.Error(fmt.Errorf("failed to submit contracts to escrow: [%v]", err))
		return
	}
	doPay(f, submitContractRes)
	return
}

func doPay(f *ds.Session, response *escrowpb.SignedSubmitContractResult) {
	f.Pay()
	privKeyStr := f.Cfg.Identity.PrivKey
	payerPrivKey, err := crypto.ToPrivKey(privKeyStr)
	if err != nil {
		f.Error(err)
		return
	}
	payerPubKey := payerPrivKey.GetPublic()
	payinRequest, err := escrow.NewPayinRequest(response, payerPubKey, payerPrivKey)
	if err != nil {
		f.Error(err)
		return
	}
	payinRes, err := escrow.PayInToEscrow(f.Ctx, f.Cfg, payinRequest)
	if err != nil {
		f.Error(fmt.Errorf("failed to pay in to escrow: [%v]", err))
		return
	}
	doGuard(f, payinRes, payerPrivKey)
}

func doGuard(f *ds.Session, res *escrowpb.SignedPayinResult, payerPriKey ic.PrivKey) {
	f.Guard()
	md, err := f.GetMetadata()
	if err != nil {
		f.Error(err)
		return
	}
	cts := make([]*guardpb.Contract, 0)
	for _, h := range md.ShardHashes {
		shard, err := ds.GetShard(f.Ctx, f.Ds, f.PeerId, f.Id, h)
		if err != nil {
			f.Error(err)
			return
		}
		contracts, err := shard.SignedCongtracts()
		if err != nil {
			f.Error(err)
			return
		}
		cts = append(cts, contracts.GuardContract)
	}
	fsStatus, err := helper.PrepAndUploadFileMeta(f.Ctx, cts, res, payerPriKey, f.Cfg, f.PeerId, md.FileHash)
	if err != nil {
		f.Error(fmt.Errorf("failed to send file meta to guard: [%v]", err))
		return
	}

	qs, err := helper.PrepFileChallengeQuestions(f.Ctx, f.N, f.Api, fsStatus, md.FileHash, f.PeerId, f.Id)
	if err != nil {
		f.Error(err)
		return
	}

	fcid, err := cidlib.Parse(md.FileHash)
	if err != nil {
		f.Error(err)
		return
	}
	err = helper.SendChallengeQuestions(f.Ctx, f.Cfg, fcid, qs)
	if err != nil {
		f.Error(fmt.Errorf("failed to send challenge questions to guard: [%v]", err))
		return
	}
	doWaitUpload(f, payerPriKey)
}

func doWaitUpload(f *ds.Session, payerPriKey ic.PrivKey) {
	f.WaitUpload()
	md, err := f.GetMetadata()
	if err != nil {
		f.Error(err)
		return
	}
	err = backoff.Retry(func() error {
		err := grpc.GuardClient(f.Cfg.Services.GuardDomain).WithContext(f.Ctx,
			func(ctx context.Context, client guardpb.GuardServiceClient) error {
				req := &guardpb.CheckFileStoreMetaRequest{
					FileHash:     md.FileHash,
					RenterPid:    md.RenterId,
					RequesterPid: f.N.Identity.Pretty(),
					RequestTime:  time.Now().UTC(),
				}
				sign, err := crypto.Sign(payerPriKey, req)
				if err != nil {
					return err
				}
				req.Signature = sign
				meta, err := client.CheckFileStoreMeta(ctx, req)
				if err != nil {
					return err
				}
				fmt.Println("meta.State", meta.State)
				if meta.State == guardpb.FileStoreStatus_RUNNING {
					return nil
				}
				return errors.New("uploading")
			})
		return err
	}, bo)
	if err != nil {
		f.Error(err)
		return
	}
	f.Complete()
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

var StorageUploadInitCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Initialize storage handshake with inquiring client.",
		ShortDescription: `
Storage host opens this endpoint to accept incoming upload/storage requests,
If current host is interested and all validation checks out, host downloads
the shard and replies back to client for the next challenge step.`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("session-id", true, false, "ID for the entire storage upload session."),
		cmds.StringArg("file-hash", true, false, "Root file storage node should fetch (the DAG)."),
		cmds.StringArg("shard-hash", true, false, "Shard the storage node should fetch."),
		cmds.StringArg("price", true, false, "Per GiB per day in BTT for storing this shard offered by client."),
		cmds.StringArg("escrow-contract", true, false, "Client's initial escrow contract data."),
		cmds.StringArg("guard-contract-meta", true, false, "Client's initial guard contract meta."),
		cmds.StringArg("storage-length", true, false, "Store file for certain length in days."),
		cmds.StringArg("shard-size", true, false, "Size of each shard received in bytes."),
		cmds.StringArg("shard-index", true, false, "Index of shard within the encoding scheme."),
	},
	RunTimeout: 5 * time.Second,
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		// check flags
		cfg, err := cmdenv.GetConfig(env)
		if err != nil {
			return err
		}
		if !cfg.Experimental.StorageHostEnabled {
			return fmt.Errorf("storage host api not enabled")
		}

		ssID := req.Arguments[0]
		fileHash, err := cidlib.Parse(req.Arguments[1])
		if err != nil {
			return err
		}
		log.Info("fileHash", fileHash)
		shardHash := req.Arguments[2]
		shardIndex, err := strconv.Atoi(req.Arguments[8])
		if err != nil {
			return err
		}
		shardSize, err := strconv.ParseInt(req.Arguments[7], 10, 64)
		if err != nil {
			return err
		}
		log.Info("shardSize", shardSize)
		price, err := strconv.ParseInt(req.Arguments[3], 10, 64)
		if err != nil {
			return err
		}
		halfSignedEscrowContBytes := req.Arguments[4]
		halfSignedGuardContBytes := req.Arguments[5]
		n, err := cmdenv.GetNode(env)
		if err != nil {
			return err
		}
		api, err := cmdenv.GetApi(env, req)
		if err != nil {
			return err
		}
		pid, ok := remote.GetStreamRequestRemotePeerID(req, n)
		if !ok {
			return fmt.Errorf("fail to get peer ID from request")
		}
		ss, err := ds.GetSession(ssID, n.Identity.Pretty(), &ds.SessionInitParams{
			Ctx:         req.Context,
			Cfg:         cfg,
			Ds:          n.Repo.Datastore(),
			N:           n,
			Api:         api,
			RenterId:    pid.Pretty(),
			FileHash:    req.Arguments[1],
			ShardHashes: []string{shardHash},
		})
		if err != nil {
			return err
		}
		shard, err := ds.GetShard(ss.Ctx, ss.Ds, ss.PeerId, ss.Id, shardHash)
		if err != nil {
			return err
		}
		settings, err := hub.GetSettings(req.Context, cfg.Services.HubDomain, n.Identity.Pretty(), n.Repo.Datastore())
		if err != nil {
			return err
		}
		if uint64(price) < settings.StoragePriceAsk {
			return fmt.Errorf("price invalid: want: >=%d, got: %d", settings.StoragePriceAsk, price)
		}
		storeLen, err := strconv.Atoi(req.Arguments[6])
		if err != nil {
			return err
		}
		if uint64(storeLen) < settings.StorageTimeMin {
			return fmt.Errorf("store length invalid: want: >=%d, got: %d", settings.StorageTimeMin, storeLen)
		}

		halfSignedGuardContract, err := guard.UnmarshalGuardContract([]byte(halfSignedGuardContBytes))
		if err != nil {
			return err
		}

		// review contract and send back to client
		halfSignedEscrowContract, err := escrow.UnmarshalEscrowContract([]byte(halfSignedEscrowContBytes))
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}

		escrowContract := halfSignedEscrowContract.GetContract()
		guardContractMeta := halfSignedGuardContract.ContractMeta
		// get renter's public key
		payerPubKey, err := pid.ExtractPublicKey()
		if err != nil {
			return err
		}
		ok, err = crypto.Verify(payerPubKey, escrowContract, halfSignedEscrowContract.GetBuyerSignature())
		if !ok || err != nil {
			return fmt.Errorf("can't verify escrow contract: %v", err)
		}
		s := halfSignedGuardContract.GetRenterSignature()
		if s == nil {
			s = halfSignedGuardContract.GetPreparerSignature()
		}
		ok, err = crypto.Verify(payerPubKey, &guardContractMeta, s)
		if !ok || err != nil {
			return fmt.Errorf("can't verify guard contract: %v", err)
		}

		// Sign on the contract
		signedEscrowContractBytes, err := escrow.SignContractAndMarshal(escrowContract, halfSignedEscrowContract, n.PrivateKey, false)
		if err != nil {
			return err
		}
		signedGuardContractBytes, err := guard.SignedContractAndMarshal(&guardContractMeta, nil,
			halfSignedGuardContract, n.PrivateKey, false, false, pid.Pretty(), pid.Pretty())
		if err != nil {
			return err
		}
		go func() {
			_, err = remote.P2PCall(req.Context, n, pid, "/storage/upload/recvcontract",
				ssID,
				shardHash,
				strconv.Itoa(shardIndex),
				signedEscrowContractBytes,
				signedGuardContractBytes,
			)
			if err != nil {
				log.Error(err)
				return
			}
			shard.Contract(&shardpb.Contracts{
				HalfSignedEscrowContract: []byte(halfSignedEscrowContBytes),
				HalfSignedGuardContract:  []byte(halfSignedGuardContBytes),
			})
		}()
		// persist file meta
		err = storage.PersistFileMetaToDatastore(n, storage.HostStoragePrefix, ssID)
		if err != nil {
			log.Error(err)
			return err
		}
		// check payment
		signedContractID, err := escrow.SignContractID(escrowContract.ContractId, n.PrivateKey)
		if err != nil {
			log.Error(err)
			return err
		}
		paidIn := make(chan bool)
		go checkPaymentFromClient(req.Context, paidIn, signedContractID, cfg)
		paid := <-paidIn
		if !paid {
			return errors.New("contract is not paid:" + escrowContract.ContractId)
		}
		contracts, err := shard.Contracts()
		if err != nil {
			return err
		}
		contract, err := guard.UnmarshalGuardContract(contracts.HalfSignedGuardContract)
		if err != nil {
			return err
		}
		downloadShardFromClient(n, api, contract)
		rr := &guardpb.ReadyForChallengeRequest{
			RenterPid:   guardContractMeta.RenterPid,
			FileHash:    guardContractMeta.FileHash,
			ShardHash:   guardContractMeta.ShardHash,
			ContractId:  guardContractMeta.ContractId,
			HostPid:     guardContractMeta.HostPid,
			PrepareTime: guardContractMeta.RentStart,
		}
		signature, err := crypto.Sign(n.PrivateKey, rr)
		if err != nil {
			return err
		}
		rr.Signature = signature
		err = grpc.GuardClient(cfg.Services.GuardDomain).WithContext(req.Context,
			func(ctx context.Context, client guardpb.GuardServiceClient) error {
				challenge, err := client.ReadyForChallenge(ctx, rr)
				fmt.Println("challenge", challenge.Code, challenge.Message)
				return err
			})
		if err != nil {
			return err
		}
		shard.Complete()
		return err
	},
}

func downloadShardFromClient(n *core.IpfsNode, api coreiface.CoreAPI, guardContract *guardpb.Contract) {
	// Need to compute a time that's fair for small vs large files
	// TODO: use backoff to achieve pause/resume cases for host downloads
	low := 30 * time.Second
	high := 5 * time.Minute
	scaled := time.Duration(float64(guardContract.ShardFileSize) / float64(units.GiB) * float64(high))
	if scaled < low {
		scaled = low
	} else if scaled > high {
		scaled = high
	}
	ctx, _ := context.WithTimeout(context.Background(), scaled)
	expir := uint64(guardContract.RentEnd.Unix())
	// Get + pin to make sure it does not get accidentally deleted
	// Sharded scheme as special pin logic to add
	// file root dag + shard root dag + metadata full dag + only this shard dag
	_, err := helper.GetChallengeResponseOrNew(ctx, n, api, guardContract.FileHash, true, expir, guardContract.ShardHash)
	if err != nil {
		log.Errorf("failed to download shard %s from file %s with contract id %s: [%v]",
			guardContract.ShardHash, guardContract.FileHash, guardContract.ContractId, err)
		return
	}
}

// call escrow service to check if payment is received or not
func checkPaymentFromClient(ctx context.Context, paidIn chan bool,
	contractID *escrowpb.SignedContractID, configuration *config.Config) {
	var err error
	paid := false
	err = backoff.Retry(func() error {
		paid, err = escrow.IsPaidin(ctx, configuration, contractID)
		if err != nil {
			return err
		}
		if paid {
			paidIn <- true
			return nil
		}
		return errors.New("reach max retry times")
	}, bo)
	if err != nil {
		log.Error("Check escrow IsPaidin failed", err)
		paidIn <- paid
	}
}

var storageUploadStatusCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Check storage upload and payment status (From client's perspective).",
		ShortDescription: `
This command print upload and payment status by the time queried.`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("session-id", true, false, "ID for the entire storage upload session.").EnableStdin(),
	},
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		status := &StatusRes{}
		// check and get session info from sessionMap
		ssID := req.Arguments[0]
		// get hosts
		cfg, err := cmdenv.GetConfig(env)
		if err != nil {
			return err
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

		session, err := ds.GetSession(ssID, n.Identity.Pretty(), &ds.SessionInitParams{
			Ctx:      req.Context,
			Cfg:      cfg,
			Ds:       n.Repo.Datastore(),
			N:        n,
			Api:      api,
			RenterId: n.Identity.Pretty(),
		})
		if err != nil {
			return err
		}
		sessionStatus, err := session.GetStatus()
		if err != nil {
			return err
		}
		status.Status = sessionStatus.Status
		status.Message = sessionStatus.Message

		// check if checking request from host or client
		if !cfg.Experimental.StorageClientEnabled && !cfg.Experimental.StorageHostEnabled {
			return fmt.Errorf("storage client/host api not enabled")
		}

		// get shards info from session
		shards := make(map[string]*ShardStatus)
		metadata, err := session.GetMetadata()
		if err != nil {
			return err
		}
		status.FileHash = metadata.FileHash
		for _, h := range metadata.ShardHashes {
			shard, err := ds.GetShard(req.Context, n.Repo.Datastore(), n.Identity.String(), session.Id, h)
			if err != nil {
				return err
			}
			st, err := shard.Status()
			if err != nil {
				return err
			}
			md, err := shard.Metadata()
			if err != nil {
				return err
			}
			c := &ShardStatus{
				ContractID: md.ContractId,
				Price:      md.Price,
				Host:       md.Receiver,
				Status:     st.Status,
				Message:    st.Message,
			}
			shards[h] = c
		}
		status.Shards = shards
		return res.Emit(status)
	},
	Type: StatusRes{},
}

type StatusRes struct {
	Status   string
	Message  string
	FileHash string
	Shards   map[string]*ShardStatus
}

type ShardStatus struct {
	ContractID string
	Price      int64
	Host       string
	Status     string
	Message    string
}
