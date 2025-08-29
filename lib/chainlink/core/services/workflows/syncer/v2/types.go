package v2

import (
	"context"

	ghcapabilities "github.com/smartcontractkit/chainlink/v2/core/services/gateway/handlers/capabilities"
	"github.com/smartcontractkit/chainlink/v2/core/services/workflows/types"
)

type SyncStrategy string

const (
	SyncStrategyReconciliation = "reconciliation"
	defaultSyncStrategy        = SyncStrategyReconciliation
)

const (
	WorkflowStatusActive uint8 = iota
	WorkflowStatusPaused
)

type Head struct {
	Hash      string
	Height    string
	Timestamp uint64
}

type Config struct {
	QueryCount   uint64
	SyncStrategy SyncStrategy
}

// FetcherFunc is an abstraction for fetching the contents stored at a URL.
type FetcherFunc func(ctx context.Context, messageID string, req ghcapabilities.Request) ([]byte, error)

type WorkflowMetadataView struct {
	WorkflowID   types.WorkflowID
	Owner        []byte
	CreatedAt    uint64
	Status       uint8
	WorkflowName string
	BinaryURL    string
	ConfigURL    string
	Tag          string
	Attributes   []byte
	DonFamily    string
}

type GetWorkflowListByDONParams struct {
	DonFamily [32]byte
	Start     uint64
	Limit     uint64
}

type GetWorkflowListByDONReturnVal struct {
	WorkflowMetadataList []WorkflowMetadataView
}

type WorkflowRegistryEventName string

var (
	// A WorkflowRegistered event represents when a workflow is registered
	WorkflowRegistered WorkflowRegistryEventName = "WorkflowRegistered"
	// A WorkflowPaused event represents when a workflow is paused
	WorkflowPaused WorkflowRegistryEventName = "WorkflowPaused"
	// A WorkflowDeleted event represents when a workflow is deleted
	WorkflowDeleted WorkflowRegistryEventName = "WorkflowDeleted"
)

type Event struct {
	Name WorkflowRegistryEventName
	Data any
}

// NOTE: The following types differ from gethwrappers in that they are chain agnostic definitions (owners are represented as bytes / workflow IDs might be more than bytes32)

type WorkflowRegisteredEvent struct {
	WorkflowID    types.WorkflowID
	WorkflowOwner []byte
	CreatedAt     uint64
	Status        uint8
	WorkflowName  string
	BinaryURL     string
	ConfigURL     string
	Tag           string
	Attributes    []byte
}

type WorkflowDeletedEvent struct {
	WorkflowID types.WorkflowID
}
