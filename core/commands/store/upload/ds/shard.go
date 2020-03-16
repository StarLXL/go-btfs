package ds

import (
	"context"
	"errors"
	"fmt"
	"github.com/TRON-US/go-btfs/core"
	"github.com/TRON-US/go-btfs/core/commands/storage"
	shardpb "github.com/TRON-US/go-btfs/core/commands/store/upload/pb/shard"
	"github.com/ipfs/go-datastore"
	"github.com/looplab/fsm"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/prometheus/common/log"
	guardpb "github.com/tron-us/go-btfs-common/protos/guard"
	"github.com/tron-us/protobuf/proto"
)

const (
	shardKeyPrefix          = "/btfs/%s/v0.0.1/renter/sessions/%s/shards/%s/"
	shardInMemKey           = shardKeyPrefix
	shardStatusKey          = shardKeyPrefix + "status"
	shardMetadataKey        = shardKeyPrefix + "metadata"
	shardContractsKey       = shardKeyPrefix + "contracts"
	shardSignedContractsKey = shardKeyPrefix + "signed-contracts"
)

var (
	shardFsmEvents = fsm.Events{
		{Name: "e-contract", Src: []string{"init"}, Dst: "contract"},
		{Name: "e-wait-upload", Src: []string{"contract"}, Dst: "wait-upload"},
		{Name: "e-complete", Src: []string{"wait-upload"}, Dst: "complete"},
	}
	shardsInMem = cmap.New()
)

type Shard struct {
	ctx        context.Context
	step       chan interface{}
	fsm        *fsm.FSM
	peerId     string
	sessionId  string
	shardHash  string
	ds         datastore.Datastore
	Contracted chan bool
}

type ShardInitParams struct {
	n                        *core.IpfsNode
	md                       *shardpb.Metadata
	halfSignedEscrowContract []byte
	halfSignGuardContract    []byte
}

func GetShard(ctx context.Context, ds datastore.Datastore, peerId string, sessionId string,
	shardHash string) (*Shard, error) {
	k := fmt.Sprintf(shardInMemKey, peerId, sessionId, shardHash)
	var s *Shard
	if tmp, ok := shardsInMem.Get(k); ok {
		s = tmp.(*Shard)
		status, err := s.Status()
		if err != nil {
			return nil, err
		}
		s.fsm.SetState(status.Status)
	} else {
		ctx := storage.NewGoContext(ctx)
		s = &Shard{
			ctx:        ctx,
			ds:         ds,
			peerId:     peerId,
			sessionId:  sessionId,
			shardHash:  shardHash,
			Contracted: make(chan bool),
		}
		s.fsm = fsm.NewFSM("init",
			shardFsmEvents,
			fsm.Callbacks{
				"enter_state": s.enterState,
			})
		shardsInMem.Set(k, s)
	}
	return s, nil
}

func (s *Shard) Init(md *shardpb.Metadata) error {
	ks := []string{
		fmt.Sprintf(shardStatusKey, s.peerId, s.sessionId, s.shardHash),
		fmt.Sprintf(shardMetadataKey, s.peerId, s.sessionId, s.shardHash),
	}
	vs := []proto.Message{
		&shardpb.Status{
			Status:  "init",
			Message: "",
		}, md,
	}
	return Batch(s.ds, ks, vs)
}

func (s *Shard) enterState(e *fsm.Event) {
	log.Info("shard:", s.shardHash, ", enter state:", e.Dst)
	switch e.Dst {
	case "contract":
		s.onContract(e.Args[0].(*shardpb.Contracts))
	case "wait-upload":
		s.onWaitUpload(e.Args[0].([]byte), e.Args[1].(*guardpb.Contract))
	case "complete":
		s.onComplete()
	}
}

func (s *Shard) onContract(sc *shardpb.Contracts) error {
	ks := []string{
		fmt.Sprintf(shardStatusKey, s.peerId, s.sessionId, s.shardHash),
		fmt.Sprintf(shardContractsKey, s.peerId, s.sessionId, s.shardHash),
	}
	vs := []proto.Message{
		&shardpb.Status{
			Status:  "contract",
			Message: "",
		}, sc,
	}
	return Batch(s.ds, ks, vs)
}

func (s *Shard) onWaitUpload(signedEscrowContractBytes []byte, gc *guardpb.Contract) error {
	status := &shardpb.Status{
		Status:  "wait-upload",
		Message: "",
	}
	ks := []string{
		fmt.Sprintf(shardStatusKey, s.peerId, s.sessionId, s.shardHash),
		fmt.Sprintf(shardSignedContractsKey, s.peerId, s.sessionId, s.shardHash),
	}
	vs := []proto.Message{
		status,
		&shardpb.SingedContracts{
			SignedEscrowContract: signedEscrowContractBytes,
			GuardContract:        gc,
		},
	}
	return Batch(s.ds, ks, vs)
}

func (s *Shard) onComplete() error {
	return Save(s.ds, fmt.Sprintf(shardStatusKey, s.peerId, s.sessionId, s.shardHash), &shardpb.Status{
		Status:  "complete",
		Message: "",
	})
}

func (s *Shard) Status() (*shardpb.Status, error) {
	st := &shardpb.Status{}
	err := Get(s.ds, fmt.Sprintf(shardStatusKey, s.peerId, s.sessionId, s.shardHash), st)
	if err == datastore.ErrNotFound {
		return st, nil
	}
	return st, err
}
func (s *Shard) Metadata() (*shardpb.Metadata, error) {
	md := &shardpb.Metadata{}
	err := Get(s.ds, fmt.Sprintf(shardMetadataKey, s.peerId, s.sessionId, s.shardHash), md)
	if err == datastore.ErrNotFound {
		return md, nil
	}
	return md, err
}

func (s *Shard) Contracts() (*shardpb.Contracts, error) {
	cg := &shardpb.Contracts{}
	err := Get(s.ds, fmt.Sprintf(shardContractsKey, s.peerId, s.sessionId, s.shardHash), cg)
	if err == datastore.ErrNotFound {
		return cg, nil
	}
	return cg, err
}

func (s *Shard) SignedContracts() (*shardpb.SingedContracts, error) {
	cg := &shardpb.SingedContracts{}
	err := Get(s.ds, fmt.Sprintf(shardSignedContractsKey, s.peerId, s.sessionId, s.shardHash), cg)
	if err == datastore.ErrNotFound {
		return cg, nil
	}
	return cg, err
}

func (s *Shard) Contract(sc *shardpb.Contracts) {
	s.fsm.Event("e-contract", sc)
}

func (s *Shard) WaitUpload(escrowContractBytes []byte, guardContract *guardpb.Contract) {
	s.fsm.Event("e-wait-upload", escrowContractBytes, guardContract)
}

func (s *Shard) Complete() {
	s.fsm.Event("e-complete")
}

func (s *Shard) Timeout() {
	s.fsm.Event("e-error", errors.New("timeout"))
}

func (s *Shard) SignedCongtracts() (*shardpb.SingedContracts, error) {
	cg := &shardpb.SingedContracts{}
	err := Get(s.ds, fmt.Sprintf(shardSignedContractsKey, s.peerId, s.sessionId, s.shardHash), cg)
	if err == datastore.ErrNotFound {
		return cg, nil
	}
	return cg, err
}
