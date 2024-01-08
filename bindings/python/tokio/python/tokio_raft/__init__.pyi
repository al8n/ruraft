from typing import AsyncIterable, Optional, Protocol, List
from datetime import timedelta
from tokio_raft import types, options, membership


class Raft:
  def reloadable_options(self) -> options.ReloadableOptions: ...
  
  def options(self) -> options.Options: ...
  
  def local_id(self) -> types.NodeId: ...
  
  def local_addr(self) -> types.NodeAddress: ...
  
  def role(self) -> types.Role: ...
  
  def current_term(self) -> int: ...
  
  def latest_membership(self) -> membership.LastMembership: ...
  
  def last_contact(self) -> Optional[timedelta]: ...
  
  def last_index(self) -> int: ...
  
  def commit_index(self) -> int: ...
  
  def applied_index(self) -> int: ...
  
  def leader(self) -> types.Node: ...

  async def leadership_watcher(self) -> AsyncIterable[bool] : ...
  
  async def leadership_change_watcher(self) -> AsyncIterable[bool] : ...
  
  async def apply(self, data: bytes) -> types.ApplyFuture : ...

  async def apply_timeout(self, timeout: timedelta) -> types.ApplyFuture : ...

  async def barrier(self) -> types.BarrierFuture: ...
  
  async def barrier_timeout(self, timeout: timedelta) -> types.BarrierFuture: ...

  async def open_snapshot(self, id: types.SnapshotId) -> SnapshotSource: ...

  async def snapshot(self) -> types.SnapshotFuture: ...

  async def snapshot_timeout(self, timeout: timedelta) -> types.SnapshotFuture: ...

  async def verify_leader(self) -> types.VerifyFuture: ...
  
  async def add_voter(self, id: types.NodeId, addr: types.NodeAddress, prev_index: int) -> types.MembershipChangeFuture:...
  
  async def add_voter_timeout(self, id: types.NodeId, addr: types.NodeAddress, prev_index: int, timeout: timedelta) -> types.MembershipChangeFuture:...
  
  async def add_nonvoter(self, id: types.NodeId, addr: types.NodeAddress, prev_index: int) -> types.MembershipChangeFuture:...

  async def add_nonvoter_timeout(self, id: types.NodeId, addr: types.NodeAddress, prev_index: int, timeout: timedelta) -> types.MembershipChangeFuture:...

  async def demote_voter(self, id: types.NodeId, prev_index: int) -> types.MembershipChangeFuture:...

  async def demote_voter_timeout(self, id: types.NodeId, prev_index: int, timeout: timedelta) -> types.MembershipChangeFuture:...
  
  async def remove(self, id: types.NodeId, prev_index: int) -> types.MembershipChangeFuture:...

  async def remove_timeout(self, id: types.NodeId, prev_index: int, timeout: timedelta) -> types.MembershipChangeFuture:...
  
  async def reload_options(self, options: options.ReloadableOptions) -> None :...
  
  async def restore(self, id: types.SnapshotId) -> None :...
  
  async def restore_timeout(self, id: types.SnapshotId, timeout: timedelta) -> None :...
  
  async def leadership_transfer(self) -> types.LeadershipTransferFuture:...
  
  async def leadership_transfer_to_node(self, id: types.NodeId, addr: types.NodeAddress) -> types.LeadershipTransferFuture:...

  async def stats(self) -> types.RaftStats:...

  async def shutdown(self) -> bool :...


class FinateStateMachineResponse(Protocol):
  def index(self) -> int: ...

class FinateStateMachineSnapshot(Protocol):
  async def persist(self, sink: SnapshotSink) -> None: ...
  
  async def release(self) -> None: ...

class FinateStateMachine(Protocol):
  async def apply(self, log: types.CommittedLog) -> FinateStateMachineResponse: ...
  
  async def apply_batch(self, logs: List[types.CommittedLog]) -> types.ApplyBatchResponse: ...

  async def snapshot(self) -> FinateStateMachineSnapshot: ...
  
  async def restore(self, source: SnapshotSource) -> None: ...


class SnapshotSource:
  @property
  def term(self) -> int: ...
  
  @property
  def index(self) -> int: ...
  
  @property
  def timestamp(self) -> int: ...
  
  @property
  def size(self) -> int: ...
  
  @property
  def membership_index(self) -> int: ...
  
  def membership(self) -> types.Membership: ...
  
  async def read(self, chunk_size: int = 1024) -> memoryview: ...
  
  async def read_exact(self, size: int) -> memoryview: ...
  
  async def read_all(self, chunk_size: int = 1024) -> memoryview: ...

  # def __aenter__(self) -> SnapshotSource: ...

  # def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

class SnapshotSink:
  def id(self) -> types.SnapshotId: ...
  
  async def write(self, data: bytes) -> None: ...
  
  async def write_all(self, data: bytes) -> None: ...

  # def __aenter__(self) -> SnapshotSink: ...

  # def __aexit__(self, exc_type, exc_value, traceback) -> None: ...