from abc import ABC, abstractmethod
from typing import List
from tokio_raft import types

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
  async def persist(self, sink: types.SnapshotSink) -> None:
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
  @abstractmethod
  async def apply(self, log: types.CommittedLog) -> FinateStateMachineResponse:
    pass

  @abstractmethod
  async def apply_batch(self, logs: List[types.CommittedLog]) -> types.ApplyBatchResponse:
    pass

  @abstractmethod
  async def snapshot(self) -> FinateStateMachineSnapshot:
    pass

  @abstractmethod
  async def restore(self, source: types.Snapshot) -> None:
    pass
