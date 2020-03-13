package upload

import (
	"fmt"
	config "github.com/TRON-US/go-btfs-config"
	"github.com/TRON-US/go-btfs/core/commands/store/upload/pb/shard"
	"github.com/TRON-US/go-btfs/core/escrow"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/prometheus/common/log"
	guardPb "github.com/tron-us/go-btfs-common/protos/guard"
)

func NewContract(md *shard.Metadata, shardHash string, configuration *config.Config,
	renterPid string) (*guardPb.ContractMeta, error) {
	guardPid, escrowPid, err := getGuardAndEscrowPid(configuration)
	if err != nil {
		return nil, err
	}
	return &guardPb.ContractMeta{
		ContractId:    md.ContractId,
		RenterPid:     renterPid,
		HostPid:       md.Receiver,
		ShardHash:     shardHash,
		ShardIndex:    md.Index,
		ShardFileSize: md.ShardFileSize,
		FileHash:      md.FileHash,
		RentStart:     md.StartTime,
		RentEnd:       md.StartTime.Add(md.ContractLength),
		GuardPid:      guardPid.Pretty(),
		EscrowPid:     escrowPid.Pretty(),
		Price:         md.Price,
		Amount:        md.TotalPay, // TODO: CHANGE and aLL other optional fields
	}, nil
}

func getGuardAndEscrowPid(configuration *config.Config) (peer.ID, peer.ID, error) {
	escrowPubKeys := configuration.Services.EscrowPubKeys
	if len(escrowPubKeys) == 0 {
		return "", "", fmt.Errorf("missing escrow public key in config")
	}
	guardPubKeys := configuration.Services.GuardPubKeys
	if len(guardPubKeys) == 0 {
		return "", "", fmt.Errorf("missing guard public key in config")
	}
	escrowPid, err := pidFromString(escrowPubKeys[0])
	if err != nil {
		log.Error("parse escrow config failed", escrowPubKeys[0])
		return "", "", err
	}
	guardPid, err := pidFromString(guardPubKeys[0])
	if err != nil {
		log.Error("parse guard config failed", guardPubKeys[1])
		return "", "", err
	}
	return guardPid, escrowPid, err
}

func pidFromString(key string) (peer.ID, error) {
	pubKey, err := escrow.ConvertPubKeyFromString(key)
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(pubKey)
}
