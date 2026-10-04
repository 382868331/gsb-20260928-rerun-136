package offlinequeue

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type Status string

const (
	StatusPending       Status = "pending"
	StatusSending       Status = "sending"
	StatusAcknowledged  Status = "acknowledged"
	StatusRejected      Status = "rejected"
	StatusBlocked       Status = "blocked"
	StatusCanceled      Status = "canceled"
	StatusCancelPending Status = "cancel_pending"
	StatusCancelDenied  Status = "cancel_denied_committed"
)

var (
	ErrAlreadyExists     = errors.New("operation already exists")
	ErrNotFound          = errors.New("operation not found")
	ErrDependencyMissing = errors.New("dependency is missing")
	ErrDependencyCycle   = errors.New("dependency cycle")
	ErrNotCancelable     = errors.New("operation is not waiting to be sent")
	ErrAlreadyCommitted  = errors.New("operation was already committed by the server")
	ErrNotRejected       = errors.New("operation is not in rejected state")
	ErrNotCommitted      = errors.New("operation was not committed by the server")
	ErrNotBlocked        = errors.New("operation is not blocked")
)

type Operation struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	Payload      string `json:"payload"`
	ObjectID     string `json:"object_id,omitempty"`
	BaseVersion  int    `json:"base_version,omitempty"`
	DependsOn    string `json:"depends_on,omitempty"`
	Revises      string `json:"revises,omitempty"`
	UndoOf       string `json:"undo_of,omitempty"`
	Status       Status `json:"status"`
	Attempts     int    `json:"attempts"`
	RejectReason string `json:"reject_reason,omitempty"`
	// MissingPremise records the original predecessor a late submission
	// depended on when that premise was already removed by consolidation.
	// The intent of the operation itself is preserved; PremisePlan names
	// the adopted consolidation scope that removed the premise.
	MissingPremise string `json:"missing_premise,omitempty"`
	PremisePlan    string `json:"premise_plan,omitempty"`
}

type State struct {
	ClientID   string                `json:"client_id"`
	Order      []string              `json:"order"`
	Operations map[string]*Operation `json:"operations"`
	Archive    []ArchivedSummary     `json:"archive,omitempty"`
	Plans      []*ConsolidationPlan  `json:"plans,omitempty"`
	PlanSeq    int                   `json:"plan_seq,omitempty"`
}

func newState(clientID string) *State {
	return &State{ClientID: clientID, Operations: make(map[string]*Operation)}
}

type FileStore struct {
	path string
}

func NewFileStore(dir, clientID string) (*FileStore, error) {
	if clientID == "" {
		return nil, errors.New("client id is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, clientID+".json")
	if data, err := os.ReadFile(path); err == nil {
		var state State
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		if state.ClientID != clientID {
			return nil, fmt.Errorf("state file belongs to client %q", state.ClientID)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return &FileStore{path: path}, nil
}

func (s *FileStore) Load(clientID string) (*State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return newState(clientID), nil
	}
	if err != nil {
		return nil, err
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *FileStore) Save(state *State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
