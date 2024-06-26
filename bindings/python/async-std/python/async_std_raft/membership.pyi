

from typing import List
from .types import NodeId, NodeAddress

  

class ServerSuffrage:
  def voter(self) -> ServerSuffrage: ...
  
  def nonvoter(self) -> ServerSuffrage: ...
  
  def is_voter(self) -> bool: ...
  
  def is_nonvoter(self) -> bool: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: ServerSuffrage) -> bool: ...

  def __ne__(self, other: ServerSuffrage) -> bool: ...

  def __hash__(self) -> int: ...

  def __int__(self) -> int: ...



class Server:
  def __init__(self, id: NodeId, address: NodeAddress, suffrage: ServerSuffrage) -> None: ...

  @property
  def id(self) -> NodeId: ...
  
  @id.setter
  def id(self, value: NodeId) -> None : ...
  
  @property
  def address(self) -> NodeAddress: ...
  
  @address.setter
  def address(self, value: NodeAddress) -> None : ...

  @property
  def suffrage(self) -> ServerSuffrage: ...

  @suffrage.setter
  def suffrage(self, value: ServerSuffrage) -> None : ...
  
  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: Server) -> bool: ...

  def __ne__(self, other: Server) -> bool: ...

  def __hash__(self) -> int: ...



class MembershipBuilder:
  def __init__(self) -> None: ...

  def insert(self, server: Server) -> None : ...
  
  def insert_many(self, servers: List[Server]) -> None : ...

  def remove(self, id: NodeId) -> None : ...

  def contains_id(self, node: NodeId) -> bool: ...
  
  def contains_addr(self, address: NodeAddress) -> bool: ...
  
  def contains_voter(self) -> bool: ...
  
  def is_voter(self, id: NodeId) -> bool: ...
  
  def is_nonvoter(self, id: NodeId) -> bool: ...

  def is_empty(self) -> bool: ...
  
  def build(self) -> Membership: ...



class Membership:
  def is_empty(self) -> bool: ...
  
  def contains_id(self, node: NodeId) -> bool: ...
  
  def contains_addr(self, address: NodeAddress) -> bool: ...
  
  def contains_voter(self) -> bool: ...
  
  def is_voter(self, id: NodeId) -> bool: ...
  
  def is_nonvoter(self, id: NodeId) -> bool: ...
  
  def num_voters(self) -> int: ...
  
  def num_nonvoters(self) -> int: ...
  
  def quorum_size(self) -> int: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: Membership) -> bool: ...

  def __ne__(self, other: Membership) -> bool: ...



class LatestMembership:
  def index(self) -> int: ...
  
  def membership(self): Membership: ...

  def __richcmp__(self, other: LatestMembership, op):...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

