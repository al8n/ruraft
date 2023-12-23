use super::*;

mod append_entries;
pub use append_entries::*;

mod install_snapshot;
pub use install_snapshot::*;

mod vote;
pub use vote::*;

mod timeout_now;
pub use timeout_now::*;

mod heartbeat;
pub use heartbeat::*;

enum_wrapper!(
  /// Request to be sent to the Raft node.
  #[derive(Debug, Clone)]
  #[non_exhaustive]
  pub enum Request<I, A, D> {
    AppendEntries(AppendEntriesRequest<I, A, D>) = 0 => append_entries,
    Vote(VoteRequest<I, A>) = 1 => vote,
    InstallSnapshot(InstallSnapshotRequest<I, A>) = 2 => install_snapshot,
    TimeoutNow(TimeoutNowRequest<I, A>) = 3 => timeout_now,
    Heartbeat(HeartbeatRequest<I, A>) = 4 => heartbeat,
  }
);

// Copy the expanded code from the macro here, because handle serde bound is anonying to deal with in
// a macro.
#[cfg(feature = "serde")]
const _: () = {
  impl<I, A, D> serde::Serialize for Request<I, A, D>
  where
    I: serde::Serialize + Id + for<'a> serde::Deserialize<'a>,
    A: serde::Serialize + Address + for<'a> serde::Deserialize<'a>,
    D: serde::Serialize + for<'a> serde::Deserialize<'a>,
  {
    fn serialize<__S>(&self, __serializer: __S) -> Result<__S::Ok, __S::Error>
    where
      __S: serde::Serializer,
    {
      match *self {
        Request::AppendEntries(ref __field0) => serde::Serialize::serialize(__field0, __serializer),
        Request::Vote(ref __field0) => serde::Serialize::serialize(__field0, __serializer),
        Request::InstallSnapshot(ref __field0) => {
          serde::Serialize::serialize(__field0, __serializer)
        }
        Request::TimeoutNow(ref __field0) => serde::Serialize::serialize(__field0, __serializer),
        Request::Heartbeat(ref __field0) => serde::Serialize::serialize(__field0, __serializer),
      }
    }
  }
  impl<'de, I, A, D> serde::Deserialize<'de> for Request<I, A, D>
  where
    I: serde::Serialize + Id + for<'a> serde::Deserialize<'a>,
    A: serde::Serialize + Address + for<'a> serde::Deserialize<'a>,
    D: serde::Serialize + for<'a> serde::Deserialize<'a>,
  {
    fn deserialize<__D>(__deserializer: __D) -> Result<Self, __D::Error>
    where
      __D: serde::Deserializer<'de>,
    {
      let __content =
        <serde::__private::de::Content as serde::Deserialize>::deserialize(__deserializer)?;
      let __deserializer =
        serde::__private::de::ContentRefDeserializer::<__D::Error>::new(&__content);
      if let Ok(__ok) = Result::map(
        <AppendEntriesRequest<I, A, D> as serde::Deserialize>::deserialize(__deserializer),
        Request::AppendEntries,
      ) {
        return Ok(__ok);
      }
      if let Ok(__ok) = Result::map(
        <VoteRequest<I, A> as serde::Deserialize>::deserialize(__deserializer),
        Request::Vote,
      ) {
        return Ok(__ok);
      }
      if let Ok(__ok) = Result::map(
        <InstallSnapshotRequest<I, A> as serde::Deserialize>::deserialize(__deserializer),
        Request::InstallSnapshot,
      ) {
        return Ok(__ok);
      }
      if let Ok(__ok) = Result::map(
        <TimeoutNowRequest<I, A> as serde::Deserialize>::deserialize(__deserializer),
        Request::TimeoutNow,
      ) {
        return Ok(__ok);
      }
      if let Ok(__ok) = Result::map(
        <HeartbeatRequest<I, A> as serde::Deserialize>::deserialize(__deserializer),
        Request::Heartbeat,
      ) {
        return Ok(__ok);
      }
      Err(serde::de::Error::custom(
        "data did not match any variant of untagged enum Request",
      ))
    }
  }
};
