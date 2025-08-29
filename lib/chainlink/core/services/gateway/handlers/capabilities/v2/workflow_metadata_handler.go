package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	jsonrpc "github.com/smartcontractkit/chainlink-common/pkg/jsonrpc2"
	"github.com/smartcontractkit/chainlink-common/pkg/logger"
	"github.com/smartcontractkit/chainlink-common/pkg/services"
	"github.com/smartcontractkit/chainlink-common/pkg/types/gateway"
	"github.com/smartcontractkit/chainlink/v2/core/services/gateway/common/aggregation"
	"github.com/smartcontractkit/chainlink/v2/core/services/gateway/config"
	"github.com/smartcontractkit/chainlink/v2/core/services/gateway/handlers"
)

const ecdsaPubKeyHexLen = 42 // 2 (0x prefix) + 40 (hex digits)

type workflowReference struct {
	workflowOwner string
	workflowName  string
	workflowTag   string
}

type WorkflowMetadataHandler struct {
	services.StateMachine
	lggr            logger.Logger
	mu              sync.RWMutex
	authorizedKeys  map[string]map[gateway.AuthorizedKey]struct{} // map of workflow ID to authorized keys
	workflowRefToID map[workflowReference]string                  // map of workflow reference to workflow ID
	workflowIDToRef map[string]workflowReference                  // map of workflow ID to workflow reference
	agg             *aggregation.WorkflowMetadataAggregator
	config          ServiceConfig
	don             handlers.DON
	donConfig       *config.DONConfig
	stopCh          services.StopChan
}

// NewWorkflowMetadataHandler creates a new WorkflowMetadataHandler.
func NewWorkflowMetadataHandler(lggr logger.Logger, cfg ServiceConfig, don handlers.DON, donConfig *config.DONConfig) *WorkflowMetadataHandler {
	// f+1 identical responses from workflow are needed for workflow metadata to be registered
	threshold := donConfig.F + 1
	return &WorkflowMetadataHandler{
		lggr:            logger.Named(lggr, "HTTPTriggerWorkflowMetadataHandler"),
		authorizedKeys:  make(map[string]map[gateway.AuthorizedKey]struct{}),
		workflowRefToID: make(map[workflowReference]string),
		workflowIDToRef: make(map[string]workflowReference),
		agg:             aggregation.NewWorkflowMetadataAggregator(lggr, threshold, time.Duration(cfg.CleanUpPeriodMs)*time.Millisecond),
		don:             don,
		donConfig:       donConfig,
		config:          cfg,
		stopCh:          make(services.StopChan),
	}
}

func (h *WorkflowMetadataHandler) Authorize(workflowID, payload, signature string) bool {
	// TODO: PRODCRE-305 Implement authorization logic
	return true
}

// syncMetadata aggregates the authorized keys and workflow selectors from the WorkflowMetadataAggregator and updates the local cache.
// Should be called periodically to keep the authorized keys up to date.
func (h *WorkflowMetadataHandler) syncMetadata() {
	metadata, err := h.agg.Aggregate()
	if err != nil {
		h.lggr.Errorw("Failed to aggregate auth data", "error", err)
		return
	}
	authorizedKeys := make(map[string]map[gateway.AuthorizedKey]struct{})
	workflowRefToID := make(map[workflowReference]string)
	workflowIDToRef := make(map[string]workflowReference)
	for _, data := range metadata {
		workflowRef := workflowReference{
			workflowOwner: data.WorkflowSelector.WorkflowOwner,
			workflowName:  data.WorkflowSelector.WorkflowName,
			workflowTag:   data.WorkflowSelector.WorkflowTag,
		}
		// Only the first aggregated workflow reference is used because
		// workflow reference is unique (enforced by workflow registry)
		// workflow reference and workflow ID mapping in the gateway eventually becomes consistent
		// with the mapping on-chain
		if _, exists := workflowIDToRef[data.WorkflowSelector.WorkflowID]; exists {
			h.lggr.Debug("Duplicate workflow ID found", "workflowID", data.WorkflowSelector.WorkflowID)
			continue
		}
		if _, exists := workflowRefToID[workflowRef]; exists {
			h.lggr.Debug("Duplicate workflow reference found", "workflowRef", workflowRef)
			continue
		}
		workflowIDToRef[data.WorkflowSelector.WorkflowID] = workflowRef
		workflowRefToID[workflowRef] = data.WorkflowSelector.WorkflowID
		authorizedKeys[data.WorkflowSelector.WorkflowID] = make(map[gateway.AuthorizedKey]struct{})
		for _, key := range data.AuthorizedKeys {
			authorizedKeys[data.WorkflowSelector.WorkflowID][key] = struct{}{}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.authorizedKeys = authorizedKeys
	h.workflowRefToID = workflowRefToID
	h.workflowIDToRef = workflowIDToRef
}

// sendMetadataPullRequest sends a request to all nodes in the DON to pull the latest metadata.
// no retries are performed, as the caller is expected to poll periodically.
func (h *WorkflowMetadataHandler) sendMetadataPullRequest(ctx context.Context) error {
	req := &jsonrpc.Request[json.RawMessage]{
		Version: jsonrpc.JsonRpcVersion,
		ID:      gateway.GetRequestID(gateway.MethodPullWorkflowMetadata),
		Method:  gateway.MethodPullWorkflowMetadata,
	}
	var combinedErr error
	for _, member := range h.donConfig.Members {
		err := h.don.SendToNode(ctx, member.Address, req)
		if err != nil {
			combinedErr = errors.Join(combinedErr, fmt.Errorf("failed to send pull request to node %s: %w", member.Address, err))
		}
	}
	return combinedErr
}

// OnMetadataPush handles the push of metadata from a node when a new workflow is registered
func (h *WorkflowMetadataHandler) OnMetadataPush(ctx context.Context, resp *jsonrpc.Response[json.RawMessage], nodeAddr string) error {
	var metadata gateway.WorkflowMetadata
	if err := json.Unmarshal(*resp.Result, &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	h.lggr.Debugw("Received metadata push", "workflowID", metadata.WorkflowSelector.WorkflowID, "nodeAddr", nodeAddr)
	err := h.validateAuthMetadata(metadata)
	if err != nil {
		return err
	}
	var combinedErr error
	err = h.agg.Collect(&metadata, nodeAddr)
	if err != nil {
		combinedErr = errors.Join(combinedErr, fmt.Errorf("failed to collect observation: %w", err))
	}
	return combinedErr
}

// OnMetadataPullResponse handles the response to the metadata pull request.
func (h *WorkflowMetadataHandler) OnMetadataPullResponse(ctx context.Context, resp *jsonrpc.Response[json.RawMessage], nodeAddr string) error {
	var metadata []gateway.WorkflowMetadata
	if err := json.Unmarshal(*resp.Result, &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata pull response: %w", err)
	}
	h.lggr.Debugw("Received metadata pull response", "nodeAddr", nodeAddr)
	for _, data := range metadata {
		err := h.validateAuthMetadata(data)
		if err != nil {
			return err
		}
	}
	var combinedErr error
	for _, data := range metadata {
		err := h.agg.Collect(&data, nodeAddr)
		combinedErr = errors.Join(combinedErr, err)
	}
	return combinedErr
}

// Start begins the periodic pull loop.
func (h *WorkflowMetadataHandler) Start(ctx context.Context) error {
	return h.StartOnce("WorkflowMetadataHandler", func() error {
		h.lggr.Info("Starting HTTP Trigger Metadata Handler")
		err := h.agg.Start(ctx)
		if err != nil {
			return err
		}
		h.runTicker(time.Duration(h.config.MetadataPullIntervalMs)*time.Millisecond, func() {
			err2 := h.sendMetadataPullRequest(ctx)
			if err2 != nil {
				h.lggr.Errorw("Failed to send pull request", "error", err2)
			}
		})
		h.runTicker(time.Duration(h.config.MetadataAggregationIntervalMs)*time.Millisecond, h.syncMetadata)
		return nil
	})
}

func (h *WorkflowMetadataHandler) runTicker(period time.Duration, fn func()) {
	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fn()
			case <-h.stopCh:
				return
			}
		}
	}()
}

func (h *WorkflowMetadataHandler) validateAuthMetadata(metadata gateway.WorkflowMetadata) error {
	if metadata.WorkflowSelector.WorkflowID == "" || metadata.WorkflowSelector.WorkflowOwner == "" || metadata.WorkflowSelector.WorkflowName == "" || metadata.WorkflowSelector.WorkflowTag == "" {
		return errors.New("invalid workflow metadata")
	}
	if len(metadata.AuthorizedKeys) == 0 {
		return errors.New("no authorized keys")
	}
	for _, key := range metadata.AuthorizedKeys {
		if key.KeyType != gateway.KeyTypeECDSA {
			return errors.New("invalid key type")
		}
		if key.PublicKey == "" || !strings.HasPrefix(key.PublicKey, "0x") || len(key.PublicKey) != ecdsaPubKeyHexLen {
			return fmt.Errorf("invalid public key: %s", key.PublicKey)
		}
		if key.PublicKey != strings.ToLower(key.PublicKey) {
			return errors.New("invalid public key: must be all lowercase")
		}
	}
	return nil
}

func (h *WorkflowMetadataHandler) Close() error {
	return h.StopOnce("WorkflowMetadataHandler", func() error {
		h.lggr.Info("Stopping HTTP Trigger Metadata Handler")
		if err := h.agg.Close(); err != nil {
			h.lggr.Errorw("Failed to close WorkflowMetadataAggregator", "error", err)
		}
		close(h.stopCh)
		return nil
	})
}
