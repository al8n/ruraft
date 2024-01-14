

from abc import ABC, abstractmethod
from typing import List
from .snapshot import *
from .types import CommittedLog

class FinateStateMachineResponse(ABC):
  """
  Response returned when the new log entries applied to the state machine.
  """

  @abstractmethod
  def index(self) -> int:
    """
    Returns the index of the newly applied log entry.
    
    Returns:
      int: The index of the newly applied log entry.
    """
    pass


class FinateStateMachineSnapshot(ABC):
  """
  Represents a snapshot of the finate state machine.
  """

  @abstractmethod
  async def persist(self, sink: SnapshotSink) -> None:
    """
    Persist should write the finate state machine snapshot to the given sink.

    Args:
      sink (SnapshotSink): _description_
    """
    pass

  @abstractmethod
  async def release(self) -> None:
    pass

class FinateStateMachine(ABC):
  """Implemented by clients to make use of the replicated log."""

  @abstractmethod
  async def apply(self, log: CommittedLog) -> FinateStateMachineResponse:
    """
    Invoked once a log entry is committed by a majority of the cluster.
  
    Apply should apply the log to the FSM. Apply must be deterministic and
    produce the same result on all peers in the cluster.
    """
    pass

  @abstractmethod
  async def apply_batch(self, logs: List[CommittedLog]) -> List[FinateStateMachineResponse]:
    """
    Invoked once a batch of log entries has been committed and
    are ready to be applied to the `FinateStateMachine`. `apply_batch` will take in an array of
    log entries. These log entries will be in the order they were committed,
    will not have gaps, and could be of a few log types.
  
    The returned slice must be the same length as the input and each response
    should correlate to the log at the same index of the input. The returned
    values will be made available in the `ApplyFuture` returned by `Raft.apply`
    method if that method was called on the same Raft node as the `FinateStateMachine`.
    """
    pass

  @abstractmethod
  async def snapshot(self) -> FinateStateMachineSnapshot:
    """
    Returns an `FinateStateMachineSnapshot` used to: support log compaction, to
    restore the `FinateStateMachine` to a previous state, or to bring out-of-date followers up
    to a recent log index.
  
    The `snapshot` implementation should return quickly, because Apply can not
    be called while `snapshot` is running. Generally this means `snapshot` should
    only capture a pointer to the state, and any expensive IO should happen
    as part of `FinateStateMachine.persist`.
  
    `apply` and `snapshot` are always called from the same thread, but `apply` will
    be called concurrently with `FinateStateMachine.persist`. This means the `FinateStateMachine` should
    be implemented to allow for concurrent updates while a snapshot is happening.
    """
    pass

  @abstractmethod
  async def restore(self, source: Snapshot) -> None:
    """
    Used to restore an `FinateStateMachine` from a snapshot. It is not called
    concurrently with any other command. The `FinateStateMachine` must discard all previous
    state before restoring the snapshot.
    """
    pass

