package ds

import (
	"context"
	"errors"
	"fmt"
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
		{Name: "e-complete", Src: []string{"init", "contract"}, Dst: "complete"},
		{Name: "e-error", Src: []string{"init", "contract"}, Dst: "error"},
	}
	shardsInMem = cmap.New()
)

type Shard struct {
	ctx       context.Context
	step      chan interface{}
	fsm       *fsm.FSM
	peerId    string
	sessionId string
	shardHash string
	ds        datastore.Datastore
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
			ctx:       ctx,
			ds:        ds,
			peerId:    peerId,
			sessionId: sessionId,
			shardHash: shardHash,
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

func (s *Shard) enterState(e *fsm.Event) {
	log.Info("shard:", s.shardHash, ", enter state:", e.Dst)
	switch e.Dst {
	case "contract":
		s.onContract(e.Args[0].(*shardpb.Contracts))
	case "complete":
		s.onComplete(e.Args[0].([]byte), e.Args[1].(*guardpb.Contract))
	case "error":
		s.onError(e.Args[0].(error))
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

func (s *Shard) onComplete(signedEscrowContractBytes []byte, gc *guardpb.Contract) error {
	status := &shardpb.Status{
		Status:  "complete",
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

func (s *Shard) onError(err error) {
	Save(s.ds, fmt.Sprintf(shardStatusKey, s.peerId, s.sessionId, s.shardHash), &shardpb.Status{
		Status:  "error",
		Message: err.Error(),
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

func (s *Shard) Complete(escrowContractBytes []byte, guardContract *guardpb.Contract) {
	s.fsm.Event("e-complete", escrowContractBytes, guardContract)
}

func (s *Shard) Error(err error) {
	s.fsm.Event("e-error", err)
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
