package v2

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/smartcontractkit/chainlink-common/pkg/custmsg"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	ghcapabilities "github.com/smartcontractkit/chainlink/v2/core/services/gateway/handlers/capabilities"

	"github.com/smartcontractkit/chainlink/v2/core/services/job"
	"github.com/smartcontractkit/chainlink/v2/core/services/keystore/keys/workflowkey"
	"github.com/smartcontractkit/chainlink/v2/core/utils"
)

type lastFetchedAtMap struct {
	m map[string]time.Time
	sync.RWMutex
}

func (l *lastFetchedAtMap) Set(url string, at time.Time) {
	l.Lock()
	defer l.Unlock()
	l.m[url] = at
}

func (l *lastFetchedAtMap) Get(url string) (time.Time, bool) {
	l.RLock()
	defer l.RUnlock()
	got, ok := l.m[url]
	return got, ok
}

func newLastFetchedAtMap() *lastFetchedAtMap {
	return &lastFetchedAtMap{
		m: map[string]time.Time{},
	}
}

func safeUint32(n uint64) uint32 {
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}

// FetcherFunc is an abstraction for fetching the contents stored at a URL.
// TODO: CAPPL-1031 refactor fetcher to use Storage service instead of Gateway
type FetcherFunc func(ctx context.Context, messageID string, req ghcapabilities.Request) ([]byte, error)

type ArtifactConfig struct {
	MaxConfigSize  uint64
	MaxSecretsSize uint64
	MaxBinarySize  uint64
}

// By default, if type is unknown, the largest artifact size is 26.4KB.  Configure the artifact size
// via the ArtifactConfig to override this default.
const defaultMaxArtifactSizeBytes = 26.4 * utils.KB

func (cfg *ArtifactConfig) ApplyDefaults() {
	if cfg.MaxConfigSize == 0 {
		cfg.MaxConfigSize = defaultMaxArtifactSizeBytes
	}
	if cfg.MaxSecretsSize == 0 {
		cfg.MaxSecretsSize = defaultMaxArtifactSizeBytes
	}
	if cfg.MaxBinarySize == 0 {
		cfg.MaxBinarySize = defaultMaxArtifactSizeBytes
	}
}

var defaultSecretsFreshnessDuration = 24 * time.Hour

func WithMaxArtifactSize(cfg ArtifactConfig) func(*Store) {
	return func(a *Store) {
		a.limits = &cfg
	}
}

type SerialisedModuleStore interface {
	StoreModule(workflowID string, binaryID string, module []byte) error
	GetModulePath(workflowID string) (string, bool, error)
	GetBinaryID(workflowID string) (string, bool, error)
	DeleteModule(workflowID string) error
}

type Store struct {
	lggr logger.Logger

	// limits sets max artifact sizes to fetch when handling events
	limits *ArtifactConfig

	orm WorkflowRegistryDS

	// fetchFn is a function that fetches the contents of a URL with a limit on the size of the response.
	fetchFn FetcherFunc

	lastFetchedAtMap         *lastFetchedAtMap
	clock                    clockwork.Clock
	secretsFreshnessDuration time.Duration

	encryptionKey workflowkey.Key

	emitter custmsg.MessageEmitter
}

func NewStore(lggr logger.Logger, orm WorkflowRegistryDS, fetchFn FetcherFunc, clock clockwork.Clock, encryptionKey workflowkey.Key,
	emitter custmsg.MessageEmitter, opts ...func(*Store)) *Store {
	limits := &ArtifactConfig{}
	limits.ApplyDefaults()

	artifactsStore := &Store{
		lggr:                     lggr,
		orm:                      orm,
		fetchFn:                  fetchFn,
		lastFetchedAtMap:         newLastFetchedAtMap(),
		clock:                    clock,
		limits:                   limits,
		secretsFreshnessDuration: defaultSecretsFreshnessDuration,
		encryptionKey:            encryptionKey,
		emitter:                  emitter,
	}

	for _, o := range opts {
		o(artifactsStore)
	}

	return artifactsStore
}

// FetchWorkflowArtifacts fetches the workflow spec and config from a cache or the specified URLs if the artifacts have not
// been cached already.  Before a workflow can be started this method must be called to ensure all artifacts used by the
// workflow are available from the store.
func (h *Store) FetchWorkflowArtifacts(ctx context.Context, workflowID, binaryURL, configURL string) ([]byte, []byte, error) {
	// Check if the workflow spec is already stored in the database
	if spec, err := h.orm.GetWorkflowSpec(ctx, workflowID); err == nil {
		// there is no update in the BinaryURL or ConfigURL, lets decode the stored artifacts
		decodedBinary, err := hex.DecodeString(spec.Workflow)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode stored workflow spec: %w", err)
		}
		return decodedBinary, []byte(spec.Config), nil
	}

	// Fetch the binary and config files from the specified URLs.
	var (
		binary, decodedBinary, config []byte
		err                           error
	)

	req := ghcapabilities.Request{
		URL:              binaryURL,
		Method:           http.MethodGet,
		MaxResponseBytes: safeUint32(h.limits.MaxBinarySize),
		WorkflowID:       workflowID,
	}
	binary, err = h.fetchFn(ctx, messageID(binaryURL, workflowID), req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch binary from %s : %w", binaryURL, err)
	}

	if decodedBinary, err = base64.StdEncoding.DecodeString(string(binary)); err != nil {
		return nil, nil, fmt.Errorf("failed to decode binary: %w", err)
	}

	if configURL != "" {
		req := ghcapabilities.Request{
			URL:              configURL,
			Method:           http.MethodGet,
			MaxResponseBytes: safeUint32(h.limits.MaxConfigSize),
			WorkflowID:       workflowID,
		}
		config, err = h.fetchFn(ctx, messageID(configURL, workflowID), req)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch config from %s : %w", configURL, err)
		}
	}
	return decodedBinary, config, nil
}

func (h *Store) GetWorkflowSpec(ctx context.Context, workflowID string) (*job.WorkflowSpec, error) {
	spec, err := h.orm.GetWorkflowSpec(ctx, workflowID)
	return spec, err
}

func (h *Store) UpsertWorkflowSpec(ctx context.Context, spec *job.WorkflowSpec) (int64, error) {
	return h.orm.UpsertWorkflowSpec(ctx, spec)
}

// DeleteWorkflowArtifacts removes the workflow spec from the database. If not found, returns nil.
func (h *Store) DeleteWorkflowArtifacts(ctx context.Context, workflowID string) error {
	err := h.orm.DeleteWorkflowSpec(ctx, workflowID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			h.lggr.Warnw("failed to delete workflow spec: not found", "workflowID", workflowID)
			return nil
		}
		return fmt.Errorf("failed to delete workflow spec: %w", err)
	}

	return nil
}

func (h *Store) GetWasmBinary(ctx context.Context, workflowID string) ([]byte, error) {
	spec, err := h.orm.GetWorkflowSpec(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow spec by workflow ID: %w", err)
	}

	// there is no update in the BinaryURL or ConfigURL, lets decode the stored artifacts
	decodedBinary, err := hex.DecodeString(spec.Workflow)
	if err != nil {
		return nil, fmt.Errorf("failed to decode stored workflow string: %w", err)
	}

	return decodedBinary, nil
}

func messageID(url string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(url))
	for _, p := range parts {
		h.Write([]byte(p))
	}
	hash := hex.EncodeToString(h.Sum(nil))
	p := []string{ghcapabilities.MethodWorkflowSyncer, hash}
	return strings.Join(p, "/")
}
