from typing import Union, Optional, List, Protocol
from datetime import timedelta
from membership import Membership
from options import ProtocolVersion, SnapshotVersion
from tokio_raft import FinateStateMachineResponse, SnapshotSource

class NodeId:
  def __init__(self, src: str) -> None: ...

  def to_bytes(self) -> bytes: ...
  
  def from_bytes(src: bytes) -> NodeId: ...

class NodeAddress:
  def __init__(self, src: str) -> None: ...

  def to_bytes(self) -> bytes: ...

  def from_bytes(src: bytes) -> NodeAddress: ...

  @property
  def port(self) -> int: ...

class Node:
  def __init__(self, id: NodeId, address: NodeAddress) -> None: ...

  @property
  def id(self) -> NodeId: ...
  
  @id.setter
  def id(self, value: NodeId) -> None : ...
  
  @property
  def address(self) -> NodeAddress: ...
  
  @address.setter
  def address(self, value: NodeAddress) -> None : ...

class Header:
  def __init__(self, id: NodeId, term: int) -> None: ...

  @property
  def id(self) -> NodeId: ...
  
  @id.setter
  def id(self, value: NodeId) -> None : ...
  
  @property
  def address(self) -> NodeAddress: ...
  
  @address.setter
  def address(self, value: NodeAddress) -> None : ...
  
  @property
  def protocol_version(self) -> ProtocolVersion: ...
  
  @protocol_version.setter
  def protocol_version(self, value: ProtocolVersion) -> None : ...

class Role:
  def __init__(self) -> None: ...

  def follower() -> Role: ...
  
  def candidate() -> Role: ...
  
  def leader() -> Role: ...
  
  def is_leader(self) -> bool: ...
  
  def is_follower(self) -> bool: ...
  
  def is_candidate(self) -> bool: ...

class CommittedLog:
  @property
  def index(self) -> int: ...
  
  @property
  def term(self) -> int: ...

  def data(self) -> Union[bytes, Membership]: ...

class RaftStats:
  @property
  def role(self) -> Role: ...
  
  @property
  def term(self) -> int: ...
  
  @property
  def last_log_index(self) -> int: ...
  
  @property
  def last_log_term(self) -> int: ...
  
  @property
  def commit_index(self) -> int: ...
  
  @property
  def applied_index(self) -> int: ...
  
  @property
  def fsm_pending(self) -> int: ...
  
  @property
  def last_snapshot_index(self) -> int: ...
  
  @property
  def last_snapshot_term(self) -> int: ...
  
  @property
  def protocol_version(self) -> ProtocolVersion: ...
  
  @property
  def snapshot_version(self) -> SnapshotVersion: ...
  
  @property
  def last_contact(self) -> Optional[timedelta]: ...

  def membership(self) -> Membership: ...

class ApplyFuture:
  async def wait(self) -> FinateStateMachineResponse: ...

class BarrierFuture:
  async def wait(self) -> FinateStateMachineResponse: ...

class MembershipChangeFuture:
  async def wait(self) -> FinateStateMachineResponse: ...

class VerifyFuture:
  async def wait(self) -> None: ...

class LeadershipTransferFuture:
  async def wait(self) -> None: ...

class AsyncRead(Protocol):
  async def read(self, chunk_size: int = 1024) -> memoryview:...

  async def read_exact(self, bytes: int) -> memoryview:...

  async def read_to_end(self, chunk_size: int = 1024) -> memoryview:...
  
  async def read_to_string(self, chunk_size: int = 1024) -> str:...

  def __aenter__(self) -> AsyncRead: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...


class AsyncReader(AsyncRead):
  def memory(self, src: bytes) -> AsyncReader: ...
  
  def file(self, path: str) -> AsyncReader: ...

  def __aenter__(self) -> AsyncReader: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

class Snapshot(AsyncRead):
  def __aenter__(self) -> Snapshot: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

class SnapshotSource:
  async def open(self) -> Snapshot: ...

class AsyncWrite(Protocol):
  async def write(self, data: bytes) -> None:...

  async def write_all(self, data: bytes) -> None:...

  async def flush(self) -> None:...

  async def close(self) -> None:...

  def __aenter__(self) -> AsyncWrite: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

class SnapshotSink(AsyncWrite):
  def id(self) -> SnapshotId: ...

  async def cancel(self) -> None: ...

  def __aenter__(self) -> SnapshotSink: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

class SnapshotFuture:
  async def wait(self) -> SnapshotSource: ...

class SnapshotId:
  @property
  def index(self) -> int: ...
  
  @property
  def term(self) -> int: ...
  
  @property
  def timestamp(self) -> int: ...

class SnapshotMeta:
  @property
  def index(self) -> int: ...
 
  @property
  def term(self) -> int: ...
  
  @property
  def timestamp(self) -> int: ...
  
  @property
  def size(self) -> int: ...
  
  @property
  def membership_index(self) -> int: ...

  def membership(self) -> Membership: ...
  
  @property
  def size(self) -> int: ...