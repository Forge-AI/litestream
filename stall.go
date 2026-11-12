package litestream

import (
	"context"
	"errors"
	"os"
	"time"
)

// ErrStalled ends a sync whose database moved no bytes for its stall window.
var ErrStalled = errors.New("the sync moved no bytes for its stall window")

func (db *DB) move() {
	db.moved.Store(time.Now().UnixNano())
}

// untilStalled returns ctx canceled with ErrStalled once neither an LTX
// encode nor an upload of db has moved a byte for stall, so a sync of any
// size runs for as long as it keeps moving.
func (db *DB) untilStalled(ctx context.Context, stall time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(ctx)
	started := time.Now().UnixNano()
	go func() {
		timer := time.NewTimer(stall)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			idle := time.Duration(time.Now().UnixNano() - max(started, db.moved.Load()))
			if idle >= stall {
				cancel(ErrStalled)
				return
			}
			timer.Reset(stall - idle)
		}
	}()
	return ctx, func() { cancel(nil) }
}

// movingFile marks db moved on every read an upload makes. It keeps Seek
// and ReadAt so clients still size the body and split it into parts, and
// leaves out WriteTo so no copy reads the file around it.
type movingFile struct {
	f  *os.File
	db *DB
}

func (m movingFile) Read(p []byte) (int, error) {
	n, err := m.f.Read(p)
	if n > 0 {
		m.db.move()
	}
	return n, err
}

func (m movingFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := m.f.ReadAt(p, off)
	if n > 0 {
		m.db.move()
	}
	return n, err
}

func (m movingFile) Seek(offset int64, whence int) (int64, error) {
	return m.f.Seek(offset, whence)
}
