package offlinequeue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// snapshot 是落盘的完整状态。每个客户端使用各自的状态文件，
// 状态复用靠的是同一份结构定义，不做任何文件同步。
type snapshot struct {
	Version int            `json:"version"`
	Ops     map[string]*Op `json:"ops"`
	Order   []string       `json:"order"`
}

// Store 负责本地操作队列与确认记录的持久化。
type Store struct {
	mu    sync.Mutex
	path  string
	ops   map[string]*Op
	order []string
}

// OpenStore 打开（不存在则创建）一个客户端自己的状态文件。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, ops: map[string]*Op{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		return s, nil
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("parse state file %s: %w", path, err)
	}
	if snap.Version != 1 {
		return nil, fmt.Errorf("unsupported state version %d", snap.Version)
	}
	s.ops = snap.Ops
	s.order = snap.Order
	if s.ops == nil {
		s.ops = map[string]*Op{}
	}
	return s, nil
}

// Lock 进入互斥区，返回的函数必须在临界区结束时调用。
func (s *Store) Lock() func() {
	s.mu.Lock()
	return s.mu.Unlock
}

// Get 返回操作副本；不存在时 ok 为 false。
func (s *Store) Get(id string) (Op, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[id]
	if !ok {
		return Op{}, false
	}
	return *op, true
}

// List 按入队顺序返回所有操作的副本。
func (s *Store) List() []Op {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Op, 0, len(s.order))
	for _, id := range s.order {
		if op, ok := s.ops[id]; ok {
			out = append(out, *op)
		}
	}
	return out
}

// add 插入新操作，重复 ID 返回错误。调用方须持锁。
func (s *Store) add(op *Op) error {
	if _, exists := s.ops[op.ID]; exists {
		return fmt.Errorf("op %q already enqueued", op.ID)
	}
	s.ops[op.ID] = op
	s.order = append(s.order, op.ID)
	return s.saveLocked()
}

// update 在持锁状态下修改操作并原子落盘。
func (s *Store) update(id string, fn func(*Op)) (Op, error) {
	op, ok := s.ops[id]
	if !ok {
		return Op{}, fmt.Errorf("op %q not found", id)
	}
	fn(op)
	if err := s.saveLocked(); err != nil {
		return Op{}, err
	}
	return *op, nil
}

// saveLocked 通过临时文件 + rename 原子写入，保证重启读到完整快照。
// 调用方须持有 mu。
func (s *Store) saveLocked() error {
	ids := make([]string, len(s.order))
	copy(ids, s.order)
	opsCopy := make(map[string]*Op, len(s.ops))
	for k, v := range s.ops {
		copied := *v
		opsCopy[k] = &copied
	}
	data, err := json.MarshalIndent(snapshot{Version: 1, Ops: opsCopy, Order: ids}, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, s.path)
}
