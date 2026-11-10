package litestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

var (
	validationChecksCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_validation_checks_total",
		Help: "Number of replica validation checks by result (success, invalid, or error).",
	}, []string{"db", "level", "result"})

	lastValidationSuccessGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_last_validation_success",
		Help: "Whether the last replica validation check succeeded (1) or failed (0). Zero before the first check completes.",
	}, []string{"db", "level"})

	lastValidationSuccessTimestampGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_last_validation_success_timestamp_seconds",
		Help: "Unix time of the last successful replica validation check. Zero until a check succeeds.",
	}, []string{"db", "level"})
)

var (
	// ErrNoCompaction is returned when no new files are available from the previous level.
	ErrNoCompaction = errors.New("no compaction")

	// ErrCompactionTooEarly is returned when a compaction is attempted too soon
	// since the last compaction time. This is used to prevent frequent
	// re-compaction when restarting the process.
	ErrCompactionTooEarly = errors.New("compaction too early")

	// ErrTxNotAvailable is returned when a transaction does not exist.
	ErrTxNotAvailable = errors.New("transaction not available")

	// ErrDBNotReady is a sentinel for errors.Is() compatibility.
	ErrDBNotReady = &DBNotReadyError{}

	// ErrShutdownInterrupted is returned when the shutdown sync retry loop
	// is interrupted by a done channel signal (e.g., second Ctrl+C).
	ErrShutdownInterrupted = errors.New("shutdown sync interrupted")

	ErrDatabaseNotFound = errors.New("database not found")
	ErrDatabaseNotOpen  = errors.New("database not open")
)

// DBNotReadyError is returned when an operation is attempted before the
// database has been initialized (e.g., page size not yet known).
type DBNotReadyError struct {
	Reason string
}

func (e *DBNotReadyError) Error() string {
	if e.Reason != "" {
		return "db not ready: " + e.Reason
	}
	return "db not ready"
}

func (e *DBNotReadyError) Is(target error) bool {
	_, ok := target.(*DBNotReadyError)
	return ok
}

// Store defaults
const (
	DefaultSnapshotInterval  = 24 * time.Hour
	DefaultSnapshotRetention = 24 * time.Hour

	DefaultRetention              = 24 * time.Hour
	DefaultRetentionCheckInterval = 1 * time.Hour

	// DefaultL0Retention is the default time that L0 files are kept around
	// after they have been compacted into L1 files.
	DefaultL0Retention = 5 * time.Minute
	// DefaultL0RetentionCheckInterval controls how frequently L0 retention is
	// enforced. This interval should be more frequent than the L1 compaction
	// interval so that VFS read replicas have time to observe new files.
	DefaultL0RetentionCheckInterval = 15 * time.Second

	// DefaultHeartbeatCheckInterval controls how frequently the heartbeat
	// monitor checks if heartbeat pings should be sent.
	DefaultHeartbeatCheckInterval = 15 * time.Second
)

// Store represents the top-level container for databases.
//
// It manages async background tasks like compactions so that the system
// is not overloaded by too many concurrent tasks.
type Store struct {
	mu     sync.Mutex
	dbs    []*DB
	levels CompactionLevels

	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()
	done   <-chan struct{}

	owed      map[int]map[*DB]struct{}
	compacted map[int]map[*DB]time.Time

	// The frequency of snapshots.
	SnapshotInterval time.Duration
	// The duration of time that snapshots are kept before being deleted.
	SnapshotRetention time.Duration

	// The duration that L0 files are kept after being compacted into L1.
	L0Retention time.Duration
	// How often to check for expired L0 files.
	L0RetentionCheckInterval time.Duration

	// If true, each replica sync pays the compactions, snapshot and retention
	// its database owes once their level is due.
	CompactionMonitorEnabled bool

	// If true, verify TXID consistency at destination level after each compaction.
	VerifyCompaction bool

	// RetentionEnabled controls whether Litestream actively deletes old files
	// during retention enforcement. When false, cloud provider lifecycle
	// policies handle retention instead. Local file cleanup still occurs.
	RetentionEnabled bool

	// Shutdown sync retry settings.
	ShutdownSyncTimeout  time.Duration
	ShutdownSyncInterval time.Duration

	// How often to check if heartbeat pings should be sent.
	HeartbeatCheckInterval time.Duration

	// Heartbeat client for health check pings. Sends pings only when
	// all databases have synced successfully within the heartbeat interval.
	Heartbeat *HeartbeatClient

	// heartbeatMonitorRunning tracks whether the heartbeat monitor goroutine is running.
	heartbeatMonitorRunning bool

	// How often to run validation checks. Zero disables periodic validation.
	ValidationInterval time.Duration

	Logger *slog.Logger
}

func NewStore(dbs []*DB, levels CompactionLevels) *Store {
	s := &Store{
		dbs:    dbs,
		levels: levels,

		SnapshotInterval:         DefaultSnapshotInterval,
		SnapshotRetention:        DefaultSnapshotRetention,
		L0Retention:              DefaultL0Retention,
		L0RetentionCheckInterval: DefaultL0RetentionCheckInterval,
		CompactionMonitorEnabled: true,
		RetentionEnabled:         true,
		ShutdownSyncTimeout:      DefaultShutdownSyncTimeout,
		ShutdownSyncInterval:     DefaultShutdownSyncInterval,
		HeartbeatCheckInterval:   DefaultHeartbeatCheckInterval,
		Logger:                   slog.Default().With(LogKeySystem, LogSystemStore),
		owed:                     map[int]map[*DB]struct{}{SnapshotLevel: {}},
		compacted:                map[int]map[*DB]time.Time{SnapshotLevel: {}},
	}
	for _, lvl := range levels {
		s.owed[lvl.Level] = map[*DB]struct{}{}
		s.compacted[lvl.Level] = map[*DB]time.Time{}
	}

	for _, db := range dbs {
		db.owe = s.ower(db)
		db.pay = func(ctx context.Context) { s.pay(ctx, db) }
		db.registered = true
		db.SetLogger(s.Logger.With(LogKeyDB, filepath.Base(db.Path())))
		db.L0Retention = s.L0Retention
		db.ShutdownSyncTimeout = s.ShutdownSyncTimeout
		db.ShutdownSyncInterval = s.ShutdownSyncInterval
		db.VerifyCompaction = s.VerifyCompaction
		db.RetentionEnabled = s.RetentionEnabled && db.RetentionEnabled
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

func (s *Store) Open(ctx context.Context) error {
	if err := s.levels.Validate(); err != nil {
		return err
	}

	initGroup, initCtx := errgroup.WithContext(ctx)
	initGroup.SetLimit(50)
	for _, db := range s.dbs {
		db := db
		initGroup.Go(func() error {
			select {
			case <-initCtx.Done():
				return initCtx.Err()
			default:
			}

			if db.Replica != nil && db.Replica.Client != nil {
				if err := db.Replica.Client.Init(initCtx); err != nil {
					return fmt.Errorf("initialize replica client for %q: %w", db.Path(), err)
				}
			}
			return nil
		})
	}
	if err := initGroup.Wait(); err != nil {
		return err
	}

	openGroup, openCtx := errgroup.WithContext(ctx)
	openGroup.SetLimit(50)
	for _, db := range s.dbs {
		db := db
		openGroup.Go(func() error {
			select {
			case <-openCtx.Done():
				return openCtx.Err()
			default:
			}
			return db.Open()
		})
	}
	if err := openGroup.Wait(); err != nil {
		return err
	}

	// Start heartbeat monitor if any database has heartbeat configured.
	s.startHeartbeatMonitorIfNeeded()

	// Start validation monitor if configured.
	if s.ValidationInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.monitorValidation(s.ctx)
		}()
	}

	return nil
}

func (s *Store) Close(ctx context.Context) (err error) {
	// Stop the store's monitors before closing the databases. Each database's
	// Close cancels the compactions, snapshots and retention its syncs pay.
	s.cancel()
	s.wg.Wait()

	s.mu.Lock()
	dbs := slices.Clone(s.dbs)
	s.mu.Unlock()

	for _, db := range dbs {
		if e := db.Close(ctx); e != nil {
			if errors.Is(e, ErrShutdownInterrupted) {
				if err == nil {
					err = e
				}
			} else if err == nil || errors.Is(err, ErrShutdownInterrupted) {
				err = e
			}
		}
	}

	return err
}

func (s *Store) DBs() []*DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.dbs)
}

// RegisterDB registers a new database with the store and starts monitoring it.
func (s *Store) RegisterDB(db *DB) error {
	if db == nil {
		return fmt.Errorf("db required")
	}

	// First check: see if database already exists
	s.mu.Lock()
	for _, existing := range s.dbs {
		if existing.Path() == db.Path() {
			s.mu.Unlock()
			return nil
		}
	}
	s.mu.Unlock()

	// Apply store-wide settings before opening the database.
	db.owe = s.ower(db)
	db.pay = func(ctx context.Context) { s.pay(ctx, db) }
	db.SetLogger(s.Logger.With(LogKeyDB, filepath.Base(db.Path())))
	db.L0Retention = s.L0Retention
	db.ShutdownSyncTimeout = s.ShutdownSyncTimeout
	db.ShutdownSyncInterval = s.ShutdownSyncInterval
	db.VerifyCompaction = s.VerifyCompaction
	db.RetentionEnabled = s.RetentionEnabled && db.RetentionEnabled
	db.Done = s.done

	// Open the database without holding the lock to avoid blocking other operations.
	// The double-check pattern below handles the race condition.
	if err := db.Open(); err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	// Second check: verify database wasn't added by another goroutine while we were opening.
	// If it was, close our instance and return without error.
	s.mu.Lock()

	for _, existing := range s.dbs {
		if existing.Path() == db.Path() {
			// Another goroutine added this database while we were opening.
			// Release lock before closing to avoid potential deadlock.
			s.mu.Unlock()
			if err := db.Close(context.Background()); err != nil {
				db.Logger.Error("close duplicate db", "path", db.Path(), "error", err)
			}
			return nil
		}
	}

	s.dbs = append(s.dbs, db)
	db.registered = true
	s.mu.Unlock()

	// Start heartbeat monitor if heartbeat is configured and monitor isn't running.
	s.startHeartbeatMonitorIfNeeded()

	return nil
}

// UnregisterDB stops monitoring the database at the provided path and closes it.
func (s *Store) UnregisterDB(ctx context.Context, path string) error {
	if path == "" {
		return fmt.Errorf("db path required")
	}

	s.mu.Lock()

	idx := -1
	var db *DB
	for i, existing := range s.dbs {
		if existing.Path() == path {
			idx = i
			db = existing
			break
		}
	}

	if db == nil {
		s.mu.Unlock()
		return nil
	}

	s.dbs = slices.Delete(s.dbs, idx, idx+1)
	db.registered = false
	for _, owed := range s.owed {
		delete(owed, db)
	}
	for _, compacted := range s.compacted {
		delete(compacted, db)
	}
	s.mu.Unlock()

	if err := db.Close(ctx); err != nil {
		return fmt.Errorf("close db: %w", err)
	}

	return nil
}

func (s *Store) ower(db *DB) func(level int) {
	return func(level int) { s.owe(db, level) }
}

func (s *Store) owe(db *DB, level int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owed, ok := s.owed[level]; ok && db.registered {
		owed[db] = struct{}{}
	}
}

func (s *Store) takeOwed(db *DB) CompactionLevels {
	s.mu.Lock()
	defer s.mu.Unlock()
	var owed CompactionLevels
	for _, lvl := range append(slices.Clone(s.levels), s.SnapshotLevel()) {
		if _, ok := s.owed[lvl.Level][db]; ok {
			delete(s.owed[lvl.Level], db)
			owed = append(owed, lvl)
		}
	}
	return owed
}

func (s *Store) pay(ctx context.Context, db *DB) {
	if !s.CompactionMonitorEnabled {
		return
	}
	db.paying.Lock()
	defer db.paying.Unlock()
	var kept CompactionLevels
	for owed := s.takeOwed(db); len(owed) > 0; owed = s.takeOwed(db) {
		for _, lvl := range owed {
			if s.payLevel(ctx, db, lvl) {
				kept = append(kept, lvl)
			}
		}
	}
	for _, lvl := range kept {
		s.owe(db, lvl.Level)
	}
}

func (s *Store) payLevel(ctx context.Context, db *DB, lvl *CompactionLevel) (stillOwed bool) {
	if !db.IsOpen() {
		return false
	}
	if lvl.Level != SnapshotLevel {
		if snapshotInfo, err := db.MaxLTXFileInfo(ctx, SnapshotLevel); err == nil && snapshotInfo.MaxTXID == 0 {
			return true
		}
	}
	_, err := s.CompactDB(ctx, db, lvl)
	switch {
	case err == nil:
	case errors.Is(err, ErrCompactionTooEarly):
		return true
	case errors.Is(err, ErrNoCompaction):
		db.Logger.Debug("no compaction", "level", lvl.Level, "path", db.Path())
		return false
	case errors.Is(err, ErrDBNotReady):
		db.Logger.Debug("db not ready, skipping", "level", lvl.Level, "path", db.Path(), "error", err)
		return false
	case isRefused(err):
		db.Logger.Warn("compaction refused; the next write retries it", "level", lvl.Level, "error", err)
		return false
	case isShutdownError(ctx, err):
		return false
	default:
		db.Logger.Error("compaction failed; the next sync retries it", "level", lvl.Level, "error", err)
		return true
	}
	if lvl.Level != SnapshotLevel {
		return false
	}
	err = s.EnforceSnapshotRetention(ctx, db)
	switch {
	case err == nil, isShutdownError(ctx, err):
	case isRefused(err):
		db.Logger.Warn("snapshot retention refused; the next snapshot retries it", "error", err)
	default:
		db.Logger.Error("retention enforcement failed; the next snapshot retries it", "error", err)
	}
	return false
}

// EnableDB starts replication for a registered database.
// The context is checked for cancellation before opening.
// Note: db.Open() itself does not support cancellation.
func (s *Store) EnableDB(ctx context.Context, path string) error {
	db := s.FindDB(path)
	if db == nil {
		return fmt.Errorf("database not found: %s", path)
	}

	if db.IsOpen() {
		return fmt.Errorf("database already enabled: %s", path)
	}

	// Check for cancellation before starting open
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("enable database: %w", err)
	}

	if err := db.Open(); err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	return nil
}

// DisableDB stops replication for a database.
func (s *Store) DisableDB(ctx context.Context, path string) error {
	db := s.FindDB(path)
	if db == nil {
		return fmt.Errorf("database not found: %s", path)
	}

	if !db.IsOpen() {
		return fmt.Errorf("database already disabled: %s", path)
	}

	if err := db.Close(ctx); err != nil {
		return fmt.Errorf("close database: %w", err)
	}

	return nil
}

// SyncDBResult holds the result of a sync operation.
type SyncDBResult struct {
	TXID           uint64
	ReplicatedTXID uint64
	Changed        bool
}

// SyncDB forces an immediate sync for a database. If wait is true, blocks
// until both WAL-to-LTX and LTX-to-remote sync complete. If checkpoint is
// true, runs one passive checkpoint, which syncs the WAL before it. Otherwise
// only performs the WAL-to-LTX sync and lets the replica monitor handle upload.
// Lock waits are context-aware: the timeout is honored while waiting for
// the database sync executor and the replica sync lock.
func (s *Store) SyncDB(ctx context.Context, path string, wait, checkpoint bool) (SyncDBResult, error) {
	db := s.FindDB(path)
	if db == nil {
		return SyncDBResult{}, fmt.Errorf("%w: %s", ErrDatabaseNotFound, path)
	}

	var phase atomic.Value
	phase.Store("is_open")
	var deadline sync.Once
	warnDeadline := func() {
		if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			return
		}
		deadline.Do(func() {
			holder := db.SyncDiagnostic()
			db.Logger.Warn("database sync deadline exceeded",
				"binary", "litestream-next",
				"path", path,
				"request_phase", phase.Load().(string),
				"wait", wait,
				"checkpoint", checkpoint,
				"holder_active", holder.Active,
				"holder_operation", holder.Operation,
				"holder_phase", holder.Phase,
				"holder_reason", holder.Reason,
				"holder_snapshotting", holder.Snapshotting,
				"holder_elapsed_seconds", holder.ElapsedSeconds,
				"holder_txid", holder.TXID,
				"holder_wal_bytes", holder.WALSize,
				"waiters", holder.ExecutorWaiterCount,
				"wait_seconds", holder.ExecutorWaitSeconds)
		})
	}
	stop := context.AfterFunc(ctx, warnDeadline)
	defer func() {
		stop()
		warnDeadline()
	}()

	if !db.IsOpen() {
		return SyncDBResult{}, fmt.Errorf("%w: %s", ErrDatabaseNotOpen, path)
	}

	phase.Store("read_position_before")
	_, beforeTXID, err := db.MaxLTX()
	if err != nil {
		return SyncDBResult{}, fmt.Errorf("read position before sync: %w", err)
	}

	switch {
	case checkpoint:
		phase.Store("checkpoint")
		if err := db.CheckpointOutlivingCaller(ctx, CheckpointModePassive); err != nil && !isSQLiteBusyError(err) {
			return SyncDBResult{}, fmt.Errorf("checkpoint database: %w", err)
		}
	case wait:
		phase.Store("sync_and_wait")
		if err := db.syncAndWait(ctx, func(stage string) { phase.Store(stage) }); err != nil {
			return SyncDBResult{}, fmt.Errorf("sync database: %w", err)
		}
	default:
		phase.Store("sync")
		if err := db.Sync(ctx); err != nil {
			return SyncDBResult{}, fmt.Errorf("sync database: %w", err)
		}
	}

	phase.Store("read_position_after")
	_, afterTXID, err := db.MaxLTX()
	if err != nil {
		return SyncDBResult{}, fmt.Errorf("read position after sync: %w", err)
	}

	var replicatedTXID uint64
	if db.Replica != nil {
		phase.Store("replica_position")
		replicatedTXID = uint64(db.Replica.Pos().TXID)
	}

	return SyncDBResult{
		TXID:           uint64(afterTXID),
		ReplicatedTXID: replicatedTXID,
		Changed:        afterTXID > beforeTXID,
	}, nil
}

// FindDB returns the database with the given path.
func (s *Store) FindDB(path string) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, db := range s.dbs {
		if db.Path() == path {
			return db
		}
	}
	return nil
}

// SetL0Retention updates the retention window for L0 files and propagates it to
// all managed databases.
func (s *Store) SetL0Retention(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.L0Retention = d
	for _, db := range s.dbs {
		db.L0Retention = d
	}
}

// SetDone sets the done channel used for interrupt handling during shutdown
// and propagates it to all managed databases.
func (s *Store) SetDone(done <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = done
	for _, db := range s.dbs {
		db.Done = done
	}
}

// SetShutdownSyncTimeout updates the shutdown sync timeout and propagates it to
// all managed databases.
func (s *Store) SetShutdownSyncTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ShutdownSyncTimeout = d
	for _, db := range s.dbs {
		db.ShutdownSyncTimeout = d
	}
}

// SetShutdownSyncInterval updates the shutdown sync interval and propagates it to
// all managed databases.
func (s *Store) SetShutdownSyncInterval(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ShutdownSyncInterval = d
	for _, db := range s.dbs {
		db.ShutdownSyncInterval = d
	}
}

// SetVerifyCompaction updates the verify compaction flag and propagates it to
// all managed databases.
func (s *Store) SetVerifyCompaction(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.VerifyCompaction = v
	for _, db := range s.dbs {
		db.VerifyCompaction = v
		db.compactor.VerifyCompaction = v
	}
}

func (s *Store) SetRetentionEnabled(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RetentionEnabled = v
	for _, db := range s.dbs {
		db.RetentionEnabled = v
		db.compactor.RetentionEnabled = v
	}
}

// SnapshotLevel returns a pseudo compaction level based on snapshot settings.
func (s *Store) SnapshotLevel() *CompactionLevel {
	return &CompactionLevel{
		Level:    SnapshotLevel,
		Interval: s.SnapshotInterval,
	}
}

// startHeartbeatMonitorIfNeeded starts the heartbeat monitor goroutine if:
// - HeartbeatCheckInterval is configured
// - Heartbeat is configured on the Store
// - The monitor is not already running
func (s *Store) startHeartbeatMonitorIfNeeded() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heartbeatMonitorRunning {
		return
	}
	if s.HeartbeatCheckInterval <= 0 {
		return
	}
	if !s.hasHeartbeatConfigLocked() {
		return
	}

	s.heartbeatMonitorRunning = true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.monitorHeartbeats(s.ctx)
	}()
}

// hasHeartbeatConfigLocked returns true if heartbeat is configured on the Store.
// Must be called with s.mu held.
func (s *Store) hasHeartbeatConfigLocked() bool {
	return s.Heartbeat != nil && s.Heartbeat.URL != ""
}

// monitorHeartbeats periodically checks if heartbeat pings should be sent.
// Heartbeat pings are only sent when ALL databases have synced successfully
// within the heartbeat interval.
func (s *Store) monitorHeartbeats(ctx context.Context) {
	s.Logger.Info("starting heartbeat monitor", "interval", s.HeartbeatCheckInterval)

	ticker := time.NewTicker(s.HeartbeatCheckInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case <-ticker.C:
		}

		s.sendHeartbeatIfNeeded(ctx)
	}
}

// sendHeartbeatIfNeeded sends a heartbeat ping if:
// - Heartbeat is configured on the Store
// - Enough time has passed since the last ping attempt
// - ALL databases have synced successfully within the heartbeat interval
func (s *Store) sendHeartbeatIfNeeded(ctx context.Context) {
	hb := s.Heartbeat
	if hb == nil || hb.URL == "" {
		return
	}

	if !hb.ShouldPing() {
		return
	}

	// Check if all databases are healthy (synced within the heartbeat interval).
	// A database is healthy if it synced within the heartbeat interval.
	healthySince := time.Now().Add(-hb.Interval)
	if !s.allDatabasesHealthy(healthySince) {
		return
	}

	// Record ping attempt time before making the request to ensure we respect
	// the configured interval even if the ping fails. This prevents rapid
	// retries that could overwhelm the endpoint.
	hb.RecordPing()

	if err := hb.Ping(ctx); err != nil {
		s.Logger.Error("heartbeat ping failed", "url", hb.URL, "error", err)
		return
	}

	s.Logger.Debug("heartbeat ping sent", "url", hb.URL)
}

// allDatabasesHealthy returns true if all databases have synced successfully
// since the given time. Returns false if there are no databases or no enabled databases.
func (s *Store) allDatabasesHealthy(since time.Time) bool {
	dbs := s.DBs()
	if len(dbs) == 0 {
		return false
	}

	enabledCount := 0
	for _, db := range dbs {
		if !db.IsOpen() {
			continue // skip disabled DBs
		}
		enabledCount++
		lastSync := db.LastSuccessfulSyncAt()
		if lastSync.IsZero() || lastSync.Before(since) {
			return false
		}
	}
	return enabledCount > 0
}

// CompactDB performs a compaction or snapshot for a given database on a single destination level.
// This function will only proceed if a compaction has not occurred before the last compaction time.
func (s *Store) CompactDB(ctx context.Context, db *DB, lvl *CompactionLevel) (*ltx.FileInfo, error) {
	// Skip if database is not yet initialized (page size unknown).
	if db.PageSize() == 0 {
		return nil, &DBNotReadyError{Reason: "page size not initialized"}
	}

	dstLevel := lvl.Level

	// Ensure we are not re-compacting before the most recent compaction time.
	prevCompactionAt := lvl.PrevCompactionAt(time.Now())
	s.mu.Lock()
	compactedAt := s.compacted[dstLevel][db]
	s.mu.Unlock()
	if !compactedAt.Before(prevCompactionAt) {
		return nil, ErrCompactionTooEarly
	}
	dstInfo, err := db.MaxLTXFileInfo(ctx, dstLevel)
	if err != nil {
		return nil, fmt.Errorf("fetch dst level info: %w", err)
	} else if dstInfo.CreatedAt.After(prevCompactionAt) {
		return nil, ErrCompactionTooEarly
	}

	// Shortcut if this is a snapshot since we are not pulling from a previous level.
	if dstLevel == SnapshotLevel {
		pos, err := db.Pos()
		if err != nil {
			return nil, fmt.Errorf("fetch db position: %w", err)
		}
		if dstInfo.MaxTXID != 0 && dstInfo.MaxTXID >= pos.TXID {
			return nil, ErrNoCompaction
		}

		info, err := db.Snapshot(ctx)
		if err != nil {
			return info, err
		}
		s.compactedIn(db, dstLevel, prevCompactionAt)
		db.Logger.InfoContext(ctx, "snapshot complete", "txid", info.MaxTXID.String(), "size", info.Size)
		return info, nil
	}

	// Fetch latest LTX files for both the source & destination so we can see if we need to make progress.
	srcLevel := s.levels.PrevLevel(dstLevel)
	srcInfo, err := db.MaxLTXFileInfo(ctx, srcLevel)
	if err != nil {
		return nil, fmt.Errorf("fetch src level info: %w", err)
	}

	// Skip if there are no new files to compact.
	if srcInfo.MaxTXID <= dstInfo.MaxTXID {
		return nil, ErrNoCompaction
	}

	if dstInfo.MaxTXID == 0 {
		snapshotInfo, err := db.MaxLTXFileInfo(ctx, SnapshotLevel)
		if err != nil {
			return nil, fmt.Errorf("fetch snapshot level info: %w", err)
		} else if srcInfo.MaxTXID <= snapshotInfo.MaxTXID {
			return nil, ErrNoCompaction
		} else if snapshotInfo.CreatedAt.After(prevCompactionAt) {
			return nil, ErrCompactionTooEarly
		}
	}

	info, err := db.Compact(ctx, dstLevel)
	if err != nil {
		return info, err
	}
	s.compactedIn(db, dstLevel, prevCompactionAt)

	db.Logger.InfoContext(ctx, "compaction complete",
		"level", dstLevel,
		slog.Group("txid",
			"min", info.MinTXID.String(),
			"max", info.MaxTXID.String(),
		),
		"size", info.Size,
	)

	return info, nil
}

func (s *Store) compactedIn(db *DB, level int, interval time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compacted[level][db] = interval
}

// EnforceSnapshotRetention removes old snapshots by timestamp and then
// cleans up all lower levels based on minimum snapshot TXID.
func (s *Store) EnforceSnapshotRetention(ctx context.Context, db *DB) error {
	// Enforce retention for the snapshot level.
	minSnapshotTXID, err := db.EnforceSnapshotRetention(ctx, time.Now().Add(-s.SnapshotRetention))
	if err != nil {
		return fmt.Errorf("enforce snapshot retention: %w", err)
	}

	// We should also enforce retention for L0 on the same schedule as L1.
	for _, lvl := range s.levels {
		// Skip L0 since it is enforced on a more frequent basis.
		if lvl.Level == 0 {
			continue
		}

		if err := db.EnforceRetentionByTXID(ctx, lvl.Level, minSnapshotTXID); err != nil {
			return fmt.Errorf("enforce L%d retention: %w", lvl.Level, err)
		}
	}

	return nil
}

// ValidationResult holds the result of validating a replica's LTX files.
type ValidationResult struct {
	Valid  bool              // true if no errors found
	Errors []ValidationError // all errors found
}

// Validate checks LTX file consistency across all databases and levels.
// SnapshotLevel (9) is excluded since snapshots are not contiguous.
func (s *Store) Validate(ctx context.Context) (*ValidationResult, error) {
	result := &ValidationResult{Valid: true}

	s.mu.Lock()
	dbs := s.dbs
	levels := s.levels
	s.mu.Unlock()

	for _, db := range dbs {
		if db.Replica == nil {
			continue
		}

		for _, lvl := range levels {
			level := strconv.Itoa(lvl.Level)
			lastSuccess := lastValidationSuccessTimestampGaugeVec.WithLabelValues(db.Path(), level)
			success := lastValidationSuccessGaugeVec.WithLabelValues(db.Path(), level)
			errs, err := db.Replica.ValidateLevel(ctx, lvl.Level)
			if err != nil {
				validationChecksCounterVec.WithLabelValues(db.Path(), level, "error").Inc()
				success.Set(0)
				return nil, fmt.Errorf("validate level %d for %s: %w", lvl.Level, db.Path(), err)
			}
			if len(errs) > 0 {
				validationChecksCounterVec.WithLabelValues(db.Path(), level, "invalid").Inc()
				success.Set(0)
				result.Valid = false
				result.Errors = append(result.Errors, errs...)
			} else {
				validationChecksCounterVec.WithLabelValues(db.Path(), level, "success").Inc()
				success.Set(1)
				lastSuccess.SetToCurrentTime()
			}
		}
	}

	return result, nil
}

// monitorValidation periodically runs validation checks on all databases.
func (s *Store) monitorValidation(ctx context.Context) {
	s.Logger.Info("starting validation monitor", "interval", s.ValidationInterval)

	ticker := time.NewTicker(s.ValidationInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case <-ticker.C:
		}

		result, err := s.Validate(ctx)
		if err != nil {
			if isShutdownError(ctx, err) {
				continue
			}
			s.Logger.Error("validation check failed", "error", err)
			continue
		}

		if !result.Valid {
			for _, verr := range result.Errors {
				s.Logger.Warn("validation error detected",
					"level", verr.Level,
					"type", verr.Type,
					"message", verr.Message,
				)
			}
		}
	}
}
