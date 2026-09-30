package offlinequeue

import (
	"errors"
	"fmt"
	"sync"
)

var ErrRemoteUnavailable = errors.New("remote service unavailable")
var ErrRejectedByService = errors.New("rejected by service")
var ErrAcknowledgementLost = errors.New("acknowledgement was lost")
var ErrStalePrecondition = errors.New("precondition version is stale")

type MockService struct {
	mu       sync.Mutex
	Offline  bool
	DropAcks map[string]int
	Reject   map[string]string
	applied  map[string]Operation
	Effects  []Operation
	versions map[string]int64
	undone   map[string]bool
}

func NewMockService() *MockService {
	return &MockService{
		DropAcks: map[string]int{},
		Reject:   map[string]string{},
		applied:  map[string]Operation{},
		versions: map[string]int64{},
		undone:   map[string]bool{},
	}
}

// SeedObject records an object version that already exists server-side,
// simulating state another client committed before this client flushes.
func (s *MockService) SeedObject(key string, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions[key] = version
}

func (s *MockService) SetOffline(offline bool) { s.Offline = offline }

func (s *MockService) Submit(op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Offline {
		return ErrRemoteUnavailable
	}
	if reason, reject := s.Reject[op.ID]; reject {
		return errors.Join(ErrRejectedByService, errors.New(reason))
	}
	if _, ok := s.applied[op.ID]; !ok {
		if op.UndoOf != "" {
			if _, targetCommitted := s.applied[op.UndoOf]; !targetCommitted {
				return errors.Join(ErrRejectedByService, errors.New("undo target never committed"))
			}
			if s.undone[op.UndoOf] {
				return ErrAlreadyUndone
			}
			s.undone[op.UndoOf] = true
			s.applied[op.ID] = op
			s.Effects = append(s.Effects, op)
		} else {
			if op.ObjectKey != "" && s.versions[op.ObjectKey] > op.BaseVersion {
				return errors.Join(ErrStalePrecondition, fmt.Errorf(
					"object %s moved from version %d to %d", op.ObjectKey, op.BaseVersion, s.versions[op.ObjectKey]))
			}
			s.applied[op.ID] = op
			s.Effects = append(s.Effects, op)
			if op.ObjectKey != "" {
				next := op.BaseVersion + 1
				if next <= s.versions[op.ObjectKey] {
					next = s.versions[op.ObjectKey] + 1
				}
				s.versions[op.ObjectKey] = next
			}
		}
	}
	if s.DropAcks[op.ID] > 0 {
		s.DropAcks[op.ID]--
		if s.DropAcks[op.ID] == 0 {
			delete(s.DropAcks, op.ID)
		}
		return ErrAcknowledgementLost
	}
	return nil
}

func (s *MockService) ObjectVersion(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.versions[key]
}

func (s *MockService) IsUndone(opID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.undone[opID]
}

func (s *MockService) Cancel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Offline {
		return ErrRemoteUnavailable
	}
	if _, applied := s.applied[id]; applied {
		return ErrAlreadyCommitted
	}
	return nil
}

func (s *MockService) EffectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Effects)
}

func (s *MockService) EffectsSnapshot() []Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Operation, len(s.Effects))
	copy(result, s.Effects)
	return result
}
