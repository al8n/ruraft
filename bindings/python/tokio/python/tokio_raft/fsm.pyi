from abc import ABC, abstractmethod
from typing import List
from tokio_raft import types

class FinateStateMachineResponse(ABC):
  @abstractmethod
  def index(self) -> int: ...

class FinateStateMachineSnapshot(ABC):
  @abstractmethod
  async def persist(self, id: types.SnapshotId, sink: types.SnapshotSink) -> None: ...

  @abstractmethod
  async def release(self) -> None: ...

class FinateStateMachine(ABC):
  @abstractmethod
  async def apply(self, log: types.CommittedLog) -> FinateStateMachineResponse: ...

  @abstractmethod
  async def apply_batch(self, logs: List[types.CommittedLog]) -> types.ApplyBatchResponse: ...

  @abstractmethod
  async def snapshot(self) -> FinateStateMachineSnapshot: ...

  @abstractmethod
  async def restore(self, source: types.AsyncReader) -> None: ...
