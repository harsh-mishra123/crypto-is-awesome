package v2

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/jonboulle/clockwork"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smartcontractkit/chainlink-common/pkg/custmsg"
	"github.com/smartcontractkit/chainlink-common/pkg/services"
	"github.com/smartcontractkit/chainlink-common/pkg/settings/limits"
	pkgworkflows "github.com/smartcontractkit/chainlink-common/pkg/workflows"

	"github.com/smartcontractkit/chainlink/v2/core/capabilities"
	"github.com/smartcontractkit/chainlink/v2/core/internal/testutils"
	"github.com/smartcontractkit/chainlink/v2/core/internal/testutils/pgtest"
	"github.com/smartcontractkit/chainlink/v2/core/internal/testutils/wasmtest"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	ghcapabilities "github.com/smartcontractkit/chainlink/v2/core/services/gateway/handlers/capabilities"
	"github.com/smartcontractkit/chainlink/v2/core/services/job"
	"github.com/smartcontractkit/chainlink/v2/core/services/keystore/keys/workflowkey"
	artifacts "github.com/smartcontractkit/chainlink/v2/core/services/workflows/artifacts/v2"
	"github.com/smartcontractkit/chainlink/v2/core/services/workflows/ratelimiter"
	"github.com/smartcontractkit/chainlink/v2/core/services/workflows/store"
	"github.com/smartcontractkit/chainlink/v2/core/services/workflows/syncer/v2/mocks"
	"github.com/smartcontractkit/chainlink/v2/core/services/workflows/syncerlimiter"
	"github.com/smartcontractkit/chainlink/v2/core/services/workflows/types"
)

type mockFetchResp struct {
	Body []byte
	Err  error
}

type mockFetcher struct {
	responseMap map[string]mockFetchResp
	calledMap   map[string]int
}

func (m *mockFetcher) Fetch(_ context.Context, mid string, req ghcapabilities.Request) ([]byte, error) {
	m.calledMap[req.URL]++
	return m.responseMap[req.URL].Body, m.responseMap[req.URL].Err
}

func (m *mockFetcher) Calls(url string) int {
	return m.calledMap[url]
}

func (m *mockFetcher) FetcherFunc() artifacts.FetcherFunc {
	return m.Fetch
}

func newMockFetcher(m map[string]mockFetchResp) *mockFetcher {
	return &mockFetcher{responseMap: m, calledMap: map[string]int{}}
}

type mockEngine struct {
	CloseErr error
	ReadyErr error
	StartErr error
}

func (m *mockEngine) Ready() error {
	return m.ReadyErr
}

func (m *mockEngine) Close() error {
	return m.CloseErr
}

func (m *mockEngine) Start(_ context.Context) error {
	return m.StartErr
}

func (m *mockEngine) HealthReport() map[string]error { return nil }

func (m *mockEngine) Name() string { return "mockEngine" }

var rlConfig = ratelimiter.Config{
	GlobalRPS:      1000.0,
	GlobalBurst:    1000,
	PerSenderRPS:   30.0,
	PerSenderBurst: 30,
}

func Test_Handler(t *testing.T) {
	lggr := logger.TestLogger(t)
	emitter := custmsg.NewLabeler()
	wfStore := store.NewInMemoryStore(lggr, clockwork.NewFakeClock())
	registry := capabilities.NewRegistry(lggr)
	registry.SetLocalRegistry(&capabilities.TestMetadataRegistry{})
	workflowEncryptionKey := workflowkey.MustNewXXXTestingOnly(big.NewInt(1))

	t.Run("fails with unsupported event type", func(t *testing.T) {
		mockORM := mocks.NewORM(t)
		ctx := testutils.Context(t)
		rl, err := ratelimiter.NewRateLimiter(rlConfig, limits.Factory{})
		require.NoError(t, err)
		workflowLimits, err := syncerlimiter.NewWorkflowLimits(lggr, syncerlimiter.Config{Global: 200, PerOwner: 200}, limits.Factory{})
		require.NoError(t, err)

		giveEvent := Event{}
		fetcher := func(_ context.Context, _ string, _ ghcapabilities.Request) ([]byte, error) {
			return []byte("contents"), nil
		}

		store := artifacts.NewStore(lggr, mockORM, fetcher, clockwork.NewFakeClock(), workflowkey.Key{}, custmsg.NewLabeler())

		h, err := NewEventHandler(lggr, wfStore, registry, NewEngineRegistry(), emitter, rl, workflowLimits, store, workflowEncryptionKey)
		require.NoError(t, err)

		err = h.Handle(ctx, giveEvent)
		require.Error(t, err)
		require.Contains(t, err.Error(), "event type unsupported")
	})
}

const (
	binaryLocation = "test/simple/cmd/testmodule.wasm"
	binaryCmd      = "core/capabilities/compute/test/simple/cmd"
)

func Test_workflowRegisteredHandler(t *testing.T) {
	var binaryURL = "http://example.com/binary"
	var configURL = "http://example.com/config"
	var config = []byte("")
	var wfOwner = []byte("0xOwner")
	var binary = wasmtest.CreateTestBinary(binaryCmd, true, t)
	var encodedBinary = []byte(base64.StdEncoding.EncodeToString(binary))
	var workflowName = "workflow-name"

	defaultValidationFn := func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler, s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, _ *mockFetcher) {
		err := h.workflowRegisteredEvent(ctx, event)
		require.NoError(t, err)

		// Verify the record is updated in the database
		dbSpec, err := s.GetWorkflowSpec(ctx, wfID.Hex())
		require.NoError(t, err)
		require.Equal(t, hex.EncodeToString(wfOwner), dbSpec.WorkflowOwner)
		require.Equal(t, wfName, dbSpec.WorkflowName)
		require.Equal(t, job.WorkflowSpecStatusActive, dbSpec.Status)

		// Verify the engine is started
		engine, ok := h.engineRegistry.Get(wfID)
		require.True(t, ok)
		err = engine.Ready()
		require.NoError(t, err)
	}

	defaultValidationFnWithFetch := func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler, s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
		defaultValidationFn(t, ctx, event, h, s, wfOwner, wfName, wfID, fetcher)

		// Verify that the URLs have been called
		require.Equal(t, 1, fetcher.Calls(event.BinaryURL))
		require.Equal(t, 1, fetcher.Calls(event.ConfigURL))
	}

	var tt = []testCase{
		{
			Name: "success with active workflow registered",
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			engineFactoryFn: func(ctx context.Context, wfid string, owner string, name types.WorkflowName, config []byte, binary []byte) (services.Service, error) {
				return &mockEngine{}, nil
			},
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL,
				}
			},
			validationFn: defaultValidationFnWithFetch,
		},
		{
			Name: "correctly generates the workflow name",
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			engineFactoryFn: func(ctx context.Context, wfid string, owner string, name types.WorkflowName, config []byte, binary []byte) (services.Service, error) {
				if _, err := hex.DecodeString(name.Hex()); err != nil {
					return nil, fmt.Errorf("invalid workflow name: %w", err)
				}
				want := hex.EncodeToString([]byte(pkgworkflows.HashTruncateName(name.String())))
				if want != name.Hex() {
					return nil, fmt.Errorf("invalid workflow name: doesn't match, got %s, want %s", name.Hex(), want)
				}
				return &mockEngine{}, nil
			},
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL}
			},
			validationFn: defaultValidationFnWithFetch,
		},
		{
			Name: "fails to start engine",
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			engineFactoryFn: func(ctx context.Context, wfid string, owner string, name types.WorkflowName, config []byte, binary []byte) (services.Service, error) {
				return &mockEngine{StartErr: assert.AnError}, nil
			},
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL}
			},
			validationFn: func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler,
				s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
				err := h.workflowRegisteredEvent(ctx, event)
				require.Error(t, err)
				require.ErrorIs(t, err, assert.AnError)
			},
		},
		{
			Name: "succeeds if correct engine already exists",
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL}
			},
			validationFn: func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler, s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
				me := &mockEngine{}
				err := h.engineRegistry.Add(wfID, me)
				require.NoError(t, err)
				err = h.workflowRegisteredEvent(ctx, event)
				require.NoError(t, err)
			},
		},
		{
			Name: "handles incorrect engine already exists",
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL}
			},
			validationFn: func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler, s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
				me := &mockEngine{}
				oldWfIDBytes := [32]byte{0, 1, 2, 3, 5}
				err := h.engineRegistry.Add(oldWfIDBytes, me)
				require.NoError(t, err)
				err = h.workflowRegisteredEvent(ctx, event)
				require.NoError(t, err)
				engineInRegistry, ok := h.engineRegistry.Get(wfID)
				assert.True(t, ok)
				require.Equal(t, engineInRegistry.WorkflowID, wfID)
			},
		},
		{
			Name: "success with paused workflow registered",
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusPaused,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL,
				}
			},
			validationFn: func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler,
				s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
				err := h.workflowRegisteredEvent(ctx, event)
				require.NoError(t, err)

				// Verify the record is updated in the database
				dbSpec, err := s.GetWorkflowSpec(ctx, wfID.Hex())
				require.NoError(t, err)
				require.Equal(t, hex.EncodeToString(wfOwner), dbSpec.WorkflowOwner)
				require.Equal(t, workflowName, dbSpec.WorkflowName)
				require.Equal(t, job.WorkflowSpecStatusPaused, dbSpec.Status)

				// Verify there is no running engine
				_, ok := h.engineRegistry.Get(wfID)
				assert.False(t, ok)
			},
		},
		{
			Name: "same wf ID, different status",
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL}
			},
			validationFn: func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler,
				s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
				// Create the record in the database
				entry := &job.WorkflowSpec{
					Workflow:      hex.EncodeToString(binary),
					Config:        string(config),
					WorkflowID:    event.WorkflowID.Hex(),
					Status:        job.WorkflowSpecStatusPaused,
					WorkflowOwner: hex.EncodeToString(event.WorkflowOwner),
					WorkflowName:  event.WorkflowName,
					SpecType:      job.WASMFile,
					BinaryURL:     event.BinaryURL,
					ConfigURL:     event.ConfigURL,
				}
				_, err := s.UpsertWorkflowSpec(ctx, entry)
				require.NoError(t, err)

				err = h.workflowRegisteredEvent(ctx, event)
				require.NoError(t, err)

				// Verify the record is updated in the database
				dbSpec, err := s.GetWorkflowSpec(ctx, wfID.Hex())
				require.NoError(t, err)
				require.Equal(t, hex.EncodeToString(wfOwner), dbSpec.WorkflowOwner)
				require.Equal(t, workflowName, dbSpec.WorkflowName)

				// This reflects the event status, not what was previously stored in the DB
				require.Equal(t, job.WorkflowSpecStatusActive, dbSpec.Status)

				_, ok := h.engineRegistry.Get(wfID)
				assert.True(t, ok)
			},
		},
		{
			Name:       "skips fetch if config url is missing",
			GiveConfig: make([]byte, 0),
			ConfigURL:  "",
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
				})
			},
			validationFn: func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler, s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
				defaultValidationFn(t, ctx, event, h, s, wfOwner, wfName, wfID, fetcher)

				// Verify that the URLs have been called
				require.Equal(t, 1, fetcher.Calls(event.BinaryURL))
				require.Equal(t, 0, fetcher.Calls(event.ConfigURL))
			},
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL}
			},
		},
		{
			Name:       "skips fetching if same DB entry exists",
			GiveConfig: config,
			ConfigURL:  configURL,
			BinaryURL:  binaryURL,
			GiveBinary: binary,
			WFOwner:    wfOwner,
			fetcherFactory: func() *mockFetcher {
				return newMockFetcher(map[string]mockFetchResp{
					binaryURL: {Body: encodedBinary, Err: nil},
					configURL: {Body: config, Err: nil},
				})
			},
			validationFn: func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler, s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher) {
				// Create the record in the database
				entry := &job.WorkflowSpec{
					Workflow:      hex.EncodeToString(binary),
					Config:        string(config),
					WorkflowID:    hex.EncodeToString(event.WorkflowID[:]),
					Status:        job.WorkflowSpecStatusActive,
					WorkflowOwner: hex.EncodeToString(event.WorkflowOwner),
					WorkflowName:  event.WorkflowName,
					SpecType:      job.WASMFile,
					BinaryURL:     event.BinaryURL,
					ConfigURL:     event.ConfigURL,
				}
				_, err := s.UpsertWorkflowSpec(ctx, entry)
				require.NoError(t, err)

				defaultValidationFn(t, ctx, event, h, s, wfOwner, wfName, wfID, fetcher)

				// Verify that the URLs have not been called
				require.Equal(t, 0, fetcher.Calls(event.BinaryURL))
				require.Equal(t, 0, fetcher.Calls(event.ConfigURL))
			},
			Event: func(wfID []byte) WorkflowRegisteredEvent {
				return WorkflowRegisteredEvent{
					Status:        WorkflowStatusActive,
					WorkflowID:    [32]byte(wfID),
					WorkflowOwner: wfOwner,
					WorkflowName:  workflowName,
					BinaryURL:     binaryURL,
					ConfigURL:     configURL,
				}
			},
		},
	}

	for _, tc := range tt {
		testRunningWorkflow(t, tc)
	}
}

type testCase struct {
	Name            string
	BinaryURL       string
	GiveBinary      []byte
	GiveConfig      []byte
	ConfigURL       string
	WFOwner         []byte
	fetcherFactory  func() *mockFetcher
	Event           func(wfID []byte) WorkflowRegisteredEvent
	validationFn    func(t *testing.T, ctx context.Context, event WorkflowRegisteredEvent, h *eventHandler, s *artifacts.Store, wfOwner []byte, wfName string, wfID types.WorkflowID, fetcher *mockFetcher)
	engineFactoryFn func(ctx context.Context, wfid string, owner string, name types.WorkflowName, config []byte, binary []byte) (services.Service, error)
}

func testRunningWorkflow(t *testing.T, tc testCase) {
	t.Helper()
	t.Run(tc.Name, func(t *testing.T) {
		var (
			ctx     = testutils.Context(t)
			lggr    = logger.TestLogger(t)
			db      = pgtest.NewSqlxDB(t)
			orm     = artifacts.NewWorkflowRegistryDS(db, lggr)
			emitter = custmsg.NewLabeler()

			binary                = tc.GiveBinary
			config                = tc.GiveConfig
			wfOwner               = tc.WFOwner
			workflowEncryptionKey = workflowkey.MustNewXXXTestingOnly(big.NewInt(1))

			fetcherFactory = tc.fetcherFactory
		)

		giveWFID, err := pkgworkflows.GenerateWorkflowID(wfOwner, "workflow-name", binary, config, "")
		require.NoError(t, err)

		event := tc.Event(giveWFID[:])

		er := NewEngineRegistry()
		opts := []func(*eventHandler){
			WithEngineRegistry(er),
		}
		if tc.engineFactoryFn != nil {
			opts = append(opts, WithEngineFactoryFn(tc.engineFactoryFn))
		}

		store := store.NewInMemoryStore(lggr, clockwork.NewFakeClock())
		registry := capabilities.NewRegistry(lggr)
		registry.SetLocalRegistry(&capabilities.TestMetadataRegistry{})
		rl, err := ratelimiter.NewRateLimiter(rlConfig, limits.Factory{})
		require.NoError(t, err)
		workflowLimits, err := syncerlimiter.NewWorkflowLimits(lggr, syncerlimiter.Config{Global: 200, PerOwner: 200}, limits.Factory{})
		require.NoError(t, err)

		fetcher := fetcherFactory()
		artifactStore := artifacts.NewStore(lggr, orm, fetcher.FetcherFunc(), clockwork.NewFakeClock(), workflowkey.Key{}, custmsg.NewLabeler())

		h, err := NewEventHandler(lggr, store, registry, NewEngineRegistry(), emitter, rl, workflowLimits, artifactStore, workflowEncryptionKey, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, h.Close()) })

		tc.validationFn(t, ctx, event, h, artifactStore, wfOwner, "workflow-name", giveWFID, fetcher)
	})
}

type mockArtifactStore struct {
	artifactStore              *artifacts.Store
	deleteWorkflowArtifactsErr error
}

func (m *mockArtifactStore) FetchWorkflowArtifacts(ctx context.Context, workflowID, binaryURL, configURL string) ([]byte, []byte, error) {
	return m.artifactStore.FetchWorkflowArtifacts(ctx, workflowID, binaryURL, configURL)
}
func (m *mockArtifactStore) GetWorkflowSpec(ctx context.Context, workflowID string) (*job.WorkflowSpec, error) {
	return m.artifactStore.GetWorkflowSpec(ctx, workflowID)
}
func (m *mockArtifactStore) UpsertWorkflowSpec(ctx context.Context, spec *job.WorkflowSpec) (int64, error) {
	return m.artifactStore.UpsertWorkflowSpec(ctx, spec)
}
func (m *mockArtifactStore) DeleteWorkflowArtifacts(ctx context.Context, workflowID string) error {
	if m.deleteWorkflowArtifactsErr != nil {
		return m.deleteWorkflowArtifactsErr
	}
	return m.artifactStore.DeleteWorkflowArtifacts(ctx, workflowID)
}
func newMockArtifactStore(as *artifacts.Store, deleteWorkflowArtifactsErr error) WorkflowArtifactsStore {
	return &mockArtifactStore{
		artifactStore:              as,
		deleteWorkflowArtifactsErr: deleteWorkflowArtifactsErr,
	}
}

func Test_workflowDeletedHandler(t *testing.T) {
	t.Run("success deleting existing engine and spec", func(t *testing.T) {
		var (
			ctx     = testutils.Context(t)
			lggr    = logger.TestLogger(t)
			db      = pgtest.NewSqlxDB(t)
			orm     = artifacts.NewWorkflowRegistryDS(db, lggr)
			emitter = custmsg.NewLabeler()

			binary                = wasmtest.CreateTestBinary(binaryCmd, true, t)
			encodedBinary         = []byte(base64.StdEncoding.EncodeToString(binary))
			config                = []byte("")
			binaryURL             = "http://example.com/binary"
			configURL             = "http://example.com/config"
			wfOwner               = []byte("0xOwner")
			workflowEncryptionKey = workflowkey.MustNewXXXTestingOnly(big.NewInt(1))

			fetcher = newMockFetcher(map[string]mockFetchResp{
				binaryURL: {Body: encodedBinary, Err: nil},
				configURL: {Body: config, Err: nil},
			})
		)

		giveWFID, err := pkgworkflows.GenerateWorkflowID(wfOwner, "workflow-name", binary, config, "")

		require.NoError(t, err)

		active := WorkflowRegisteredEvent{
			Status:        WorkflowStatusActive,
			WorkflowID:    giveWFID,
			WorkflowOwner: wfOwner,
			WorkflowName:  "workflow-name",
			BinaryURL:     binaryURL,
			ConfigURL:     configURL}

		er := NewEngineRegistry()
		store := store.NewInMemoryStore(lggr, clockwork.NewFakeClock())
		registry := capabilities.NewRegistry(lggr)
		registry.SetLocalRegistry(&capabilities.TestMetadataRegistry{})
		rl, err := ratelimiter.NewRateLimiter(rlConfig, limits.Factory{})
		require.NoError(t, err)
		workflowLimits, err := syncerlimiter.NewWorkflowLimits(lggr, syncerlimiter.Config{Global: 200, PerOwner: 200}, limits.Factory{})
		require.NoError(t, err)

		artifactStore := artifacts.NewStore(lggr, orm, fetcher.FetcherFunc(), clockwork.NewFakeClock(), workflowkey.Key{}, custmsg.NewLabeler())

		h, err := NewEventHandler(lggr, store, registry, NewEngineRegistry(), emitter, rl, workflowLimits, artifactStore, workflowEncryptionKey, WithEngineRegistry(er))
		require.NoError(t, err)
		err = h.workflowRegisteredEvent(ctx, active)
		require.NoError(t, err)

		// Verify the record is updated in the database
		dbSpec, err := orm.GetWorkflowSpec(ctx, types.WorkflowID(giveWFID).Hex())
		require.NoError(t, err)
		require.Equal(t, hex.EncodeToString(wfOwner), dbSpec.WorkflowOwner)
		require.Equal(t, "workflow-name", dbSpec.WorkflowName)
		require.Equal(t, job.WorkflowSpecStatusActive, dbSpec.Status)

		// Verify the engine is started
		engine, ok := h.engineRegistry.Get(types.WorkflowID(giveWFID))
		assert.True(t, ok)
		err = engine.Ready()
		require.NoError(t, err)

		deleteEvent := WorkflowDeletedEvent{
			WorkflowID: giveWFID,
		}
		err = h.workflowDeletedEvent(ctx, deleteEvent)
		require.NoError(t, err)

		// Verify the record is deleted in the database
		_, err = orm.GetWorkflowSpec(ctx, types.WorkflowID(giveWFID).Hex())
		require.Error(t, err)

		// Verify the engine is deleted
		_, ok = h.engineRegistry.Get(types.WorkflowID(giveWFID))
		assert.False(t, ok)
	})

	t.Run("success deleting non-existing workflow spec", func(t *testing.T) {
		var (
			ctx     = testutils.Context(t)
			lggr    = logger.TestLogger(t)
			db      = pgtest.NewSqlxDB(t)
			orm     = artifacts.NewWorkflowRegistryDS(db, lggr)
			emitter = custmsg.NewLabeler()

			binary                = wasmtest.CreateTestBinary(binaryCmd, true, t)
			encodedBinary         = []byte(base64.StdEncoding.EncodeToString(binary))
			config                = []byte("")
			binaryURL             = "http://example.com/binary"
			configURL             = "http://example.com/config"
			wfOwner               = []byte("0xOwner")
			workflowEncryptionKey = workflowkey.MustNewXXXTestingOnly(big.NewInt(1))

			fetcher = newMockFetcher(map[string]mockFetchResp{
				binaryURL: {Body: encodedBinary, Err: nil},
				configURL: {Body: config, Err: nil},
			})
		)

		giveWFID, err := pkgworkflows.GenerateWorkflowID(wfOwner, "workflow-name", binary, config, "")
		require.NoError(t, err)

		er := NewEngineRegistry()
		store := store.NewInMemoryStore(lggr, clockwork.NewFakeClock())
		registry := capabilities.NewRegistry(lggr)
		registry.SetLocalRegistry(&capabilities.TestMetadataRegistry{})
		rl, err := ratelimiter.NewRateLimiter(rlConfig, limits.Factory{})
		require.NoError(t, err)
		workflowLimits, err := syncerlimiter.NewWorkflowLimits(lggr, syncerlimiter.Config{Global: 200, PerOwner: 200}, limits.Factory{})
		require.NoError(t, err)
		artifactStore := artifacts.NewStore(lggr, orm, fetcher.FetcherFunc(), clockwork.NewFakeClock(), workflowkey.Key{}, custmsg.NewLabeler())

		h, err := NewEventHandler(lggr, store, registry, NewEngineRegistry(), emitter, rl, workflowLimits, artifactStore, workflowEncryptionKey, WithEngineRegistry(er))
		require.NoError(t, err)

		deleteEvent := WorkflowDeletedEvent{
			WorkflowID: giveWFID,
		}
		err = h.workflowDeletedEvent(ctx, deleteEvent)
		require.NoError(t, err)

		// Verify the record is deleted in the database
		_, err = orm.GetWorkflowSpec(ctx, types.WorkflowID(giveWFID).Hex())
		require.Error(t, err)
	})

	t.Run("removes from DB before engine registry", func(t *testing.T) {
		var (
			ctx     = testutils.Context(t)
			lggr    = logger.TestLogger(t)
			db      = pgtest.NewSqlxDB(t)
			orm     = artifacts.NewWorkflowRegistryDS(db, lggr)
			emitter = custmsg.NewLabeler()

			binary                = wasmtest.CreateTestBinary(binaryCmd, true, t)
			encodedBinary         = []byte(base64.StdEncoding.EncodeToString(binary))
			config                = []byte("")
			binaryURL             = "http://example.com/binary"
			configURL             = "http://example.com/config"
			wfOwner               = []byte("0xOwner")
			workflowEncryptionKey = workflowkey.MustNewXXXTestingOnly(big.NewInt(1))

			fetcher = newMockFetcher(map[string]mockFetchResp{
				binaryURL: {Body: encodedBinary, Err: nil},
				configURL: {Body: config, Err: nil},
			})

			failWith = "mocked fail DB delete"
		)

		giveWFID, err := pkgworkflows.GenerateWorkflowID(wfOwner, "workflow-name", binary, config, "")

		require.NoError(t, err)

		active := WorkflowRegisteredEvent{
			Status:        WorkflowStatusActive,
			WorkflowID:    giveWFID,
			WorkflowOwner: wfOwner,
			WorkflowName:  "workflow-name",
			BinaryURL:     binaryURL,
			ConfigURL:     configURL}

		er := NewEngineRegistry()
		store := store.NewInMemoryStore(lggr, clockwork.NewFakeClock())
		registry := capabilities.NewRegistry(lggr)
		registry.SetLocalRegistry(&capabilities.TestMetadataRegistry{})
		rl, err := ratelimiter.NewRateLimiter(rlConfig, limits.Factory{})
		require.NoError(t, err)
		workflowLimits, err := syncerlimiter.NewWorkflowLimits(lggr, syncerlimiter.Config{Global: 200, PerOwner: 200}, limits.Factory{})
		require.NoError(t, err)

		artifactStore := artifacts.NewStore(lggr, orm, fetcher.FetcherFunc(), clockwork.NewFakeClock(), workflowkey.Key{}, custmsg.NewLabeler())

		mockAS := newMockArtifactStore(artifactStore, errors.New(failWith))

		h, err := NewEventHandler(lggr, store, registry, NewEngineRegistry(), emitter, rl, workflowLimits, mockAS, workflowEncryptionKey, WithEngineRegistry(er))
		require.NoError(t, err)
		err =
			h.workflowRegisteredEvent(ctx, active)
		require.NoError(t, err)

		// Verify the record is updated in the database
		dbSpec, err := orm.GetWorkflowSpec(ctx, types.WorkflowID(giveWFID).Hex())
		require.NoError(t, err)
		require.Equal(t, hex.EncodeToString(wfOwner), dbSpec.WorkflowOwner)
		require.Equal(t, "workflow-name", dbSpec.WorkflowName)
		require.Equal(t, job.WorkflowSpecStatusActive, dbSpec.Status)

		// Verify the engine is started
		engine, ok := h.engineRegistry.Get(types.WorkflowID(giveWFID))
		assert.True(t, ok)
		err = engine.Ready()
		require.NoError(t, err)

		deleteEvent := WorkflowDeletedEvent{
			WorkflowID: giveWFID,
		}
		err = h.workflowDeletedEvent(ctx, deleteEvent)
		require.Error(t, err, failWith)

		// Verify the record is still in the DB
		_, err = orm.GetWorkflowSpec(ctx, types.WorkflowID(giveWFID).Hex())
		require.NoError(t, err)

		// Verify the engine is still running
		_, ok = h.engineRegistry.Get(giveWFID)
		assert.True(t, ok)
	})
}
