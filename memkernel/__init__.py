"""MemKernel: an event-sourced memory kernel for multi-agent coding systems."""

from memkernel.core.events import EventEnvelope, EventType
from memkernel.core.locks import LockManager
from memkernel.core.notices import CoordinationNotice, NoticeBoard
from memkernel.core.store import AsyncEventStore, BranchExistsError, BranchNotFoundError, StaleHeadError
from memkernel.kernel import DEFAULT_BRANCH, FencingTokenError, LeaseGrant, MemKernel
from memkernel.memory.gc import GCConfig, GCResult, HeuristicSummarizer, LLMSummarizer, MemoryGCEngine
from memkernel.memory.state_view import WorkspaceState
from memkernel.memory.token_counter import TokenCounter

__version__ = "0.3.0"

__all__ = [
    "AsyncEventStore",
    "BranchExistsError",
    "BranchNotFoundError",
    "CoordinationNotice",
    "DEFAULT_BRANCH",
    "EventEnvelope",
    "EventType",
    "FencingTokenError",
    "GCConfig",
    "GCResult",
    "HeuristicSummarizer",
    "LLMSummarizer",
    "LeaseGrant",
    "LockManager",
    "MemKernel",
    "MemoryGCEngine",
    "NoticeBoard",
    "StaleHeadError",
    "TokenCounter",
    "WorkspaceState",
    "__version__",
]
