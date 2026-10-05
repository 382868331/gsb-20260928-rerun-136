package offlinequeue

import (
	"errors"
	"sync"
)

var ErrRemoteUnavailable = errors.New("remote service unavailable")
var ErrRejectedByService = errors.New("rejected by service")
var ErrAcknowledgementLost = errors.New("acknowledgement was lost")
var ErrPreconditionFailed = errors.New("precondition version is stale")

type MockService struct {
	mu       sync.Mutex
	Offline  bool
	DropAcks map[string]int
	Reject   map[string]string
	applied  map[string]Operation
	Versions map[string]int
	Effects  []Operation
}

func NewMockService() *MockService {
	return &MockService{DropAcks: map[string]int{}, Reject: map[string]string{}, applied: map[string]Operation{}, Versions: map[string]int{}}
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
		if op.ObjectID != "" && op.BaseVersion != s.Versions[op.ObjectID] {
			return errors.Join(ErrRejectedByService, ErrPreconditionFailed,
				errors.New("object "+op.ObjectID+" version mismatch"))
		}
		s.applied[op.ID] = op
		s.Effects = append(s.Effects, op)
		if op.ObjectID != "" {
			s.Versions[op.ObjectID]++
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

// Verify reports whether the operation's effect is already applied
// remotely, so an in-flight operation is never re-executed blindly.
func (s *MockService) Verify(opID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Offline {
		return false, ErrRemoteUnavailable
	}
	_, applied := s.applied[opID]
	return applied, nil
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

func (s *MockService) Version(objectID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Versions[objectID]
}
