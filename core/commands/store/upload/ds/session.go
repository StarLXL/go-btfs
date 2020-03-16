package ds

import (
	"context"
	"fmt"
	"time"

	"github.com/TRON-US/go-btfs/core"
	"github.com/TRON-US/go-btfs/core/commands/storage"
	sessionpb "github.com/TRON-US/go-btfs/core/commands/store/upload/pb/session"
	"github.com/TRON-US/go-btfs/core/escrow"
	"github.com/tron-us/protobuf/proto"

	config "github.com/TRON-US/go-btfs-config"
	coreiface "github.com/TRON-US/interface-go-btfs-core"
	escrowpb "github.com/tron-us/go-btfs-common/protos/escrow"

	"github.com/cenkalti/backoff/v3"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/looplab/fsm"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/prometheus/common/log"
)

const (
	sessionKeyPrefix   = "/btfs/%s/v0.0.1/renter/sessions/%s/"
	sessionInMemKey    = sessionKeyPrefix
	sessionMetadataKey = sessionKeyPrefix + "metadata"
	session_status_key = sessionKeyPrefix + "status"
)

var (
	sessionsInMem = cmap.New()
	fsmEvents     = fsm.Events{
		{Name: "e-submit", Src: []string{"init"}, Dst: "submit"},
		{Name: "e-pay", Src: []string{"submit"}, Dst: "pay"},
		{Name: "e-guard", Src: []string{"pay"}, Dst: "guard"},
		{Name: "e-wait-upload", Src: []string{"guard"}, Dst: "wait-upload"},
		{Name: "e-complete", Src: []string{"wait-upload"}, Dst: "complete"},
		{Name: "e-error", Src: []string{"init", "submit", "pay", "guard", "wait-upload"}, Dst: "error"},
	}
	bo = func() *backoff.ExponentialBackOff {
		bo := backoff.NewExponentialBackOff()
		bo.InitialInterval = 10 * time.Second
		bo.MaxElapsedTime = 24 * time.Hour
		bo.Multiplier = 1.5
		bo.MaxInterval = 5 * time.Minute
		return bo
	}()
)

type Session struct {
	Id     string
	PeerId string
	Ctx    context.Context
	Cfg    *config.Config
	fsm    *fsm.FSM
	Ds     datastore.Datastore
	N      *core.IpfsNode
	Api    coreiface.CoreAPI
}

type SessionInitParams struct {
	Ctx         context.Context
	Cfg         *config.Config
	Ds          datastore.Datastore
	N           *core.IpfsNode
	Api         coreiface.CoreAPI
	RenterId    string
	FileHash    string
	ShardHashes []string
}

func GetSession(sessionId string, peerId string, params *SessionInitParams) (*Session, error) {
	if sessionId == "" {
		sessionId = uuid.New().String()
	}
	k := fmt.Sprintf(sessionInMemKey, peerId, peerId, sessionId)
	var s *Session
	if tmp, ok := sessionsInMem.Get(k); ok {
		s = tmp.(*Session)
		status, err := s.GetStatus()
		if err != nil {
			return nil, err
		}
		s.fsm.SetState(status.Status)
	} else {
		ctx := storage.NewGoContext(params.Ctx)
		s = &Session{
			Id:     sessionId,
			PeerId: peerId,
			Ctx:    ctx,
			Cfg:    params.Cfg,
			Ds:     params.Ds,
			N:      params.N,
			Api:    params.Api,
		}
		s.fsm = fsm.NewFSM("init", fsmEvents, fsm.Callbacks{
			"enter_state": s.enterState,
		})
		s.init(params)
		sessionsInMem.Set(k, s)
	}
	return s, nil
}

func (f *Session) GetStatus() (*sessionpb.Status, error) {
	sk := fmt.Sprintf(session_status_key, f.PeerId, f.Id)
	st := &sessionpb.Status{}
	err := Get(f.Ds, sk, st)
	if err == datastore.ErrNotFound {
		return st, nil
	}
	return st, err
}

func (f *Session) GetMetadata() (*sessionpb.Metadata, error) {
	mk := fmt.Sprintf(sessionMetadataKey, f.PeerId, f.Id)
	md := &sessionpb.Metadata{}
	err := Get(f.Ds, mk, md)
	if err == datastore.ErrNotFound {
		return md, nil
	}
	return md, err
}

func (f *Session) enterState(e *fsm.Event) {
	log.Info("session:", f.Id, ",enter state:", e.Dst)
	msg := ""
	switch e.Dst {
	case "error":
		msg = e.Args[0].(error).Error()
	}
	f.setStatus(e.Dst, msg)
}

func (f *Session) Submit() {
	f.fsm.Event("e-submit")
}

func (f *Session) Pay() {
	f.fsm.Event("e-pay")
}

func (f *Session) Guard() {
	f.fsm.Event("e-guard")
}

func (f *Session) WaitUpload() {
	f.fsm.Event("e-wait-upload")
}

func (f *Session) Complete() {
	f.fsm.Event("e-complete")
}

func (f *Session) Error(err error) {
	f.fsm.Event("e-error", err)
}

func (f *Session) init(params *SessionInitParams) error {
	status := &sessionpb.Status{
		Status:  "init",
		Message: "",
	}
	metadata := &sessionpb.Metadata{
		TimeCreate:  time.Now().UTC(),
		RenterId:    params.RenterId,
		FileHash:    params.FileHash,
		ShardHashes: params.ShardHashes,
	}
	ks := []string{fmt.Sprintf(session_status_key, f.PeerId, f.Id),
		fmt.Sprintf(sessionMetadataKey, f.PeerId, f.Id),
	}
	vs := []proto.Message{
		status,
		metadata,
	}
	return Batch(f.Ds, ks, vs)
}

func (f *Session) setStatus(s string, msg string) error {
	status := &sessionpb.Status{
		Status:  s,
		Message: msg,
	}
	return Save(f.Ds, fmt.Sprintf(session_status_key, f.PeerId, f.Id), status)
}

func (s *Session) PrepareContractFromShard() ([]*escrowpb.SignedEscrowContract, int64, error) {
	var signedContracts []*escrowpb.SignedEscrowContract
	var totalPrice int64
	md, err := s.GetMetadata()
	if err != nil {
		return nil, 0, err
	}
	for _, hash := range md.ShardHashes {
		shard, err := GetShard(s.Ctx, s.Ds, s.PeerId, s.Id, hash)
		if err != nil {
			return nil, 0, err
		}
		scs, err := shard.SignedCongtracts()
		if err != nil {
			return nil, 0, err
		}
		smd, err := shard.Metadata()
		if err != nil {
			return nil, 0, err
		}
		sc, err := escrow.UnmarshalEscrowContract(scs.SignedEscrowContract)
		if err != nil {
			return nil, 0, err
		}
		signedContracts = append(signedContracts, sc)
		totalPrice += smd.TotalPay
	}
	return signedContracts, totalPrice, nil
}

func (f *Session) GetCompleteShardsNum() (int, int, error) {
	md, err := f.GetMetadata()
	var completeNum, errorNum int
	if err != nil {
		return 0, 0, err
	}
	for _, h := range md.ShardHashes {
		shard, err := GetShard(f.Ctx, f.Ds, f.PeerId, f.Id, h)
		if err != nil {
			continue
		}
		status, err := shard.Status()
		if err != nil {
			continue
		}
		if status.Status == "complete" {
			completeNum++
		} else if status.Status == "error" {
			errorNum++
			return completeNum, errorNum, nil
		}
	}
	return completeNum, errorNum, nil
}
