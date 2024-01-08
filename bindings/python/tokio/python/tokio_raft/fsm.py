from abc import ABC, abstractmethod
from typing import List
from tokio_raft import types

class AsyncClose(ABC):
  """
  Abstract base class representing an asynchronous close operation.

  This class provides an interface for asynchronous close operations,
  allowing resources like files or network connections to be closed asynchronously.
  """

  @abstractmethod
  async def close(self) -> None:
    """
    Close the resource asynchronously.

    This method performs any necessary cleanup and closes the resource. 
    It should be called when the resource is no longer needed.

    Returns:
      None

    Raises:
      IOError: If an error occurs during the closing process.
    """
    pass


class AsyncWrite(AsyncClose, ABC):
  """
  Abstract base class representing an asynchronous writer.

  This class provides an interface for asynchronous writing operations,
  allowing for writing data to a destination asynchronously.
  """

  @abstractmethod
  async def write(self, data: bytes) -> None:
    """
    Write data asynchronously.

    This method writes the given bytes to the destination. It may not
    write all bytes and needs to be called repeatedly until all data
    is written.

    Args:
      data (bytes): The data to be written.

    Returns:
      None

    Raises:
      IOError: If the destination is closed or an error occurs.
    """
    pass

  @abstractmethod
  async def write_all(self, data: bytes) -> None:
    """
    Write all data asynchronously.

    This method writes the entire given bytes to the destination. It
    continues writing until all data is written or an error occurs.

    Args:
      data (bytes): The data to be written.

    Returns:
      None

    Raises:
      IOError: If the destination is closed or an error occurs before all data is written.
    """
    pass

class AsyncRead(AsyncClose, ABC):
  """
  Abstract base class representing an asynchronous reader.

  This class provides an interface for asynchronous reading operations,
  allowing for reading data in various ways.
  """

  @abstractmethod
  async def read(self, chunk_size: int = 1024) -> memoryview:
    """
    Read a chunk of data asynchronously.

    Args:
      chunk_size (int, optional): The maximum number of bytes to read. Defaults to 1024.

    Returns:
      memoryview: A memoryview object containing the bytes read.

    Raises:
      IOError: If the source is closed or an error occurs during reading.
    """
    pass

  @abstractmethod
  async def read_exact(self, size: int) -> memoryview:
    """
    Read exactly `size` bytes from the source asynchronously.

    Args:
      size (int): The number of bytes to read.

    Returns:
      memoryview: A memoryview object containing exactly `size` bytes.

    Raises:
      IOError: If the source is closed or an error occurs during reading.
    """
    pass

  @abstractmethod
  async def read_all(self, chunk_size: int = 1024) -> memoryview:
    """
    Read all available data asynchronously, in chunks of `chunk_size`.

    Args:
      chunk_size (int, optional): The size of each chunk to read. Defaults to 1024.

    Returns:
      memoryview: A memoryview object containing all bytes read.

    Raises:
      IOError: If the source is closed or an error occurs during reading.
    """
    pass


class SnapshotSink(AsyncWrite, ABC):
  """
  The :class:`FinateStateMachine` will write state to the sink. On error, :meth:`cancel` will be invoked.
  """

  @abstractmethod
  def id(self) -> types.SnapshotId:
    """
    Returns the snapshot id for the parent snapshot.
    
    Returns:
      types.SnapshotId: The snapshot id.
    """
    pass
  
  @abstractmethod
  async def cancel(self) -> None:
    """
    Cancel the sink.
    
    Raises:
      IOError: If an error occurs during cancellation.
    """
    pass

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
  async def restore(self, source: AsyncRead) -> None:
    pass
