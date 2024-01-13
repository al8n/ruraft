

from abc import ABC, abstractmethod
from typing import List
from .snapshot import *
from .types import CommittedLog

class FinateStateMachineResponse(ABC):
  @abstractmethod
  def index(self) -> int: ...

class FinateStateMachineSnapshot(ABC):
  @abstractmethod
  async def persist(self, sink: SnapshotSink) -> None: ...

  @abstractmethod
  async def release(self) -> None: ...

class FinateStateMachine(ABC):
  @abstractmethod
  async def apply(self, log: CommittedLog) -> FinateStateMachineResponse: ...

  @abstractmethod
  async def apply_batch(self, logs: List[CommittedLog]) -> List[FinateStateMachineResponse]: ...

  @abstractmethod
  async def snapshot(self) -> FinateStateMachineSnapshot: ...

  @abstractmethod
  async def restore(self, source: Snapshot) -> None: ...
  
