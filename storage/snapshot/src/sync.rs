use std::{
  fs::{self, File},
  hash::Hasher,
  io::{self, BufReader, BufWriter, Read, Seek, Write},
  mem,
  path::{Path, PathBuf},
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

use agnostic::Runtime;
use futures::AsyncWriteExt;
use once_cell::sync::Lazy;
use ruraft_core::{
  membership::Membership,
  options::SnapshotVersion,
  storage::{SnapshotId, SnapshotMeta, SnapshotSink, SnapshotSource, SnapshotStorage},
  transport::{Address, Id, Transformable},
};
use ruraft_utils::{io::ChecksumableWriter, make_dir_all};

const TEST_PATH: &str = "perm_test";
const SNAPSHOT_PATH: &str = "snapshots";
static META_FILE_PATH: Lazy<PathBuf> = Lazy::new(|| {
  let mut path = PathBuf::from("meta");
  path.set_extension("json");
  path
});
static STATE_FILE_PATH: Lazy<PathBuf> = Lazy::new(|| {
  let mut path = PathBuf::from("state");
  path.set_extension("bin");
  path
});
const TEMP_SUFFIX: &str = ".tmp";

trait SnapshotIdExt {
  fn name(&self) -> String;
  fn temp_name(&self) -> String;
}

impl SnapshotIdExt for SnapshotId {
  fn name(&self) -> String {
    format!("{}_{}_{}", self.term(), self.index(), self.timestamp())
  }

  fn temp_name(&self) -> String {
    format!(
      "{}_{}_{}{TEMP_SUFFIX}",
      self.term(),
      self.index(),
      self.timestamp()
    )
  }
}

/// Errors returned by the [`FileSnapshotStorage`].
#[derive(Debug, thiserror::Error)]
pub enum FileSnapshotStorageError {
  /// Returned when trying to build a file snapshot storage with 0 retain.
  #[error("must retain at least one snapshot")]
  InvalidRetain,
  /// Returned when the snapshot path is not accessible.
  #[error("snapshot path not accessible: {0}")]
  PathNotAccessible(io::Error),
  /// Returned when trying to build a file snapshot storage, and fail the permissions test.
  #[error("permissions test failed: {0}")]
  NoPermissions(io::Error),

  /// Returned when reading the snapshot, checksum does not match.
  #[error("checksum mismatch")]
  ChecksumMismatch,

  /// IO error
  #[error("{0}")]
  IO(#[from] io::Error),
}

/// Options use to create a [`FileSnapshotStorage`].
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone)]
pub struct FileSnapshotStorageOptions {
  /// The base directory to store snapshots in.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the base directory")),
    setter(attrs(doc = "Set the base directory"))
  )]
  base: PathBuf,
  /// The `retain` controls how many
  /// snapshots are retained. Must be at least 1.
  #[viewit(
    getter(const, attrs(doc = "Get the number of snapshots should be retained")),
    setter(attrs(doc = "Set the number of snapshots should be retained"))
  )]
  retain: usize,
}

impl FileSnapshotStorageOptions {
  /// Create a new `FileSnapshotStorageOptions`.
  pub fn new<P: AsRef<Path>>(base: P, retain: usize) -> Self {
    Self {
      base: base.as_ref().to_path_buf(),
      retain,
    }
  }
}

/// Implements the [`SnapshotStorage`] trait and allows
/// snapshots to be made on the local disk.
#[derive(Clone)]
pub struct FileSnapshotStorage<I, A, R> {
  path: Arc<PathBuf>,
  retain: usize,

  /// `no_sync`, if true, skips crash-safe file fsync api calls.
  /// It's a private field, only used in testing
  no_sync: bool,

  _runtime: std::marker::PhantomData<(I, A, R)>,
}

impl<I, A, R> FileSnapshotStorage<I, A, R>
where
  I: Id + Send + Sync + Unpin + 'static,
  I::Error: Send + Sync + Unpin + 'static,
  A: Address + Send + Sync + Unpin + 'static,
  A::Error: Send + Sync + Unpin + 'static,
  R: Runtime,
{
  /// Reaps any snapshots beyond the retain count.
  pub fn reap_snapshots(&self) -> io::Result<()> {
    let snapshots = self.get_snapshots().map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to get snapshots");
      e
    })?;

    for snap in snapshots.iter().skip(self.retain) {
      let path = self.path.join(snap.id().name());
      tracing::info!(target = "ruraft.snapshot.file", path = %path.display(), "reaping snapshot");
      fs::remove_dir_all(&path).map_err(|e| {
        tracing::error!(target = "ruraft.snapshot.file", path = %path.display(), err = %e, "failed to reap snapshot");
        e
      })?;
    }
    Ok(())
  }

  fn check_permissions(&self) -> io::Result<()> {
    let path = self.path.join(TEST_PATH);
    {
      File::create(&path)?;
    }

    fs::remove_file(&path)
  }

  fn get_snapshots(&self) -> io::Result<Vec<SnapshotMeta<I, A>>> {
    // Get the eligible snapshots
    let snapshots = fs::read_dir(self.path.as_path()).map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to scan snapshot directory");
      e
    })?;
    let mut res = Vec::with_capacity(snapshots.size_hint().0);

    // populate the metadata
    for snap in snapshots {
      let snap = snap?;
      // Ignore any files
      if !snap.file_type()?.is_dir() {
        continue;
      }

      // Ignore any temporary snapshots
      let dirname = snap.file_name();
      let dirname_str = dirname.to_string_lossy();
      if dirname_str.ends_with(TEMP_SUFFIX) {
        tracing::warn!(target = "ruraft.snapshot.file", name = %dirname_str, "found temporary snapshot");
        continue;
      }

      // Try to read the meta data
      let meta = match self.read_meta(dirname_str.as_ref()) {
        Ok(meta) => meta,
        Err(e) => {
          tracing::error!(target = "ruraft.snapshot.file", name = %dirname_str, err = %e, "failed to read snapshot metadata");
          continue;
        }
      };

      // Make sure we can understand this version.
      if !meta.meta.version.valid() {
        let version = meta.meta.version as u8;
        tracing::warn!(target = "ruraft.snapshot.file", name = %dirname_str, version = %version, "snapshot version not supported");
        continue;
      }

      // Append, but only return up to the retain count
      res.push(meta.meta);
    }

    // Sort the snapshot, reverse so we get new -> old
    res.sort_by(|a, b| {
      a.term.cmp(&b.term).then_with(|| {
        a.index
          .cmp(&b.index)
          .then_with(|| a.timestamp().cmp(&b.timestamp))
      })
    });
    res.reverse();
    Ok(res)
  }

  fn read_meta(&self, name: &str) -> io::Result<FileSnapshotMeta<I, A>> {
    // Open the meta file
    let metapath = self.path.join(name).join(META_FILE_PATH.as_path());
    let mut fh = BufReader::new(File::open(metapath)?);

    // Read the meta data
    <FileSnapshotMeta<I, A> as Transformable>::decode_from_reader(&mut fh)
      .map(|(_, meta)| meta)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
  }
}

impl<I, A, R> SnapshotStorage for FileSnapshotStorage<I, A, R>
where
  I: Id + Send + Sync + Unpin + 'static,
  I::Error: Send + Sync + Unpin + 'static,
  A: Address + Send + Sync + Unpin + 'static,
  A::Error: Send + Sync + Unpin + 'static,
  R: Runtime,
{
  type Error = FileSnapshotStorageError;
  type Sink = FileSnapshotSink<Self::Id, Self::Address, R>;
  type Id = I;
  type Address = A;
  type Runtime = R;
  type Source = FileSnapshotSource<Self::Id, Self::Address, R>;
  type Options = FileSnapshotStorageOptions;

  async fn new(opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    let FileSnapshotStorageOptions { base, retain } = opts;
    if retain < 1 {
      return Err(FileSnapshotStorageError::InvalidRetain);
    }

    // Ensure our path exists
    let path = base.join(SNAPSHOT_PATH);
    make_dir_all(&path, 0o755).map_err(FileSnapshotStorageError::PathNotAccessible)?;

    // Setup the store
    let this = Self {
      path: Arc::new(path),
      retain,
      no_sync: false,
      _runtime: std::marker::PhantomData,
    };

    this
      .check_permissions()
      .map(|_| this)
      .map_err(FileSnapshotStorageError::NoPermissions)
  }

  async fn create(
    &self,
    version: SnapshotVersion,
    index: u64,
    term: u64,
    membership: Membership<Self::Id, Self::Address>,
    membership_index: u64,
  ) -> Result<Self::Sink, Self::Error> {
    // Create a new path
    let id = SnapshotId::new(index, term);
    let path = self.path.join(id.temp_name());

    tracing::info!(
      target = "ruraft.snapshot.file",
      "creating new snapshot at {}",
      path.display()
    );

    // make the directory
    make_dir_all(&path, 0o755).map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to make snapshot directly");
      e
    })?;

    // Create the sink
    let meta = FileSnapshotMeta {
      meta: SnapshotMeta {
        version,
        timestamp: id.timestamp(),
        index,
        term,
        membership,
        membership_index,
        size: 0,
      },
      crc: 0,
    };

    FileSnapshotSink::<Self::Id, Self::Address, Self::Runtime>::write_meta(
      &path,
      &meta,
      self.no_sync,
    )
    .map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to write metadata");
      e
    })?;

    // Open the state file

    let state_path = path.join(STATE_FILE_PATH.as_path());
    let state_file = File::create(state_path).map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to create state file");
      e
    })?;
    let w = BufWriter::new(ChecksumableWriter::new(
      state_file,
      crc32fast::Hasher::new(),
    ));

    let this = FileSnapshotSink {
      store: self.clone(),
      dir: path,
      no_sync: self.no_sync,
      file: w,
      meta,
      closed: false,
    };

    Ok(this)
  }

  /// Used to list the available snapshots in the store.
  /// It should return then in descending order, with the highest index first.
  async fn list(&self) -> Result<Vec<SnapshotMeta<Self::Id, Self::Address>>, Self::Error> {
    // Get the eligible snapshots
    let mut snapshots = self.get_snapshots().map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to get snapshots");
      e
    })?;

    if snapshots.len() > self.retain {
      snapshots.drain(self.retain..);
    }
    Ok(snapshots)
  }

  /// Open takes a snapshot ID and provides a ReadCloser.
  async fn open(&self, id: &SnapshotId) -> Result<Self::Source, Self::Error> {
    let filename = id.name();
    // Get the metadata
    let meta = self.read_meta(filename.as_str()).map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to get meta data to open snapshot");
      e
    })?;

    // Open the state file
    let state_path = self.path.join(&filename).join(STATE_FILE_PATH.as_path());

    let mut state_file = File::open(state_path).map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to open state file");
      e
    })?;

    // Create a CRC64 hasher
    let mut data = Vec::new();
    state_file.read_to_end(&mut data)?;
    let mut hash = crc32fast::Hasher::new();
    hash.write(&data);
    let crc = hash.finish();

    if meta.crc != crc {
      tracing::error!(target = "ruraft.snapshot.file", stored = %meta.crc, computed = %crc, "checksum mismatch");
      return Err(FileSnapshotStorageError::ChecksumMismatch);
    }

    // Seek to the start
    state_file.seek(io::SeekFrom::Start(0)).map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "state file seek failed");
      e
    })?;

    Ok(FileSnapshotSource {
      meta,
      file: BufReader::new(state_file),
      _runtime: std::marker::PhantomData,
    })
  }
}

/// Stored on disk. We also put a CRC
/// on disk so that we can verify the snapshot.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct FileSnapshotMeta<I, A> {
  #[cfg_attr(
    feature = "serde",
    serde(
      flatten,
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  meta: SnapshotMeta<I, A>,
  crc: u64,
}

/// The [`SnapshotSource`] implementor for [`FileSnapshotStorage`].
pub struct FileSnapshotSource<I, A, R> {
  meta: FileSnapshotMeta<I, A>,
  file: BufReader<File>,
  _runtime: std::marker::PhantomData<R>,
}

impl<I, A, R> futures::io::AsyncRead for FileSnapshotSource<I, A, R>
where
  I: Send + Sync + Unpin + 'static,
  A: Send + Sync + Unpin + 'static,
  R: Send + Sync + Unpin + 'static,
{
  fn poll_read(
    mut self: Pin<&mut Self>,
    _cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    Poll::Ready(self.file.read(buf))
  }
}

impl<I, A, R> SnapshotSource for FileSnapshotSource<I, A, R>
where
  I: Id + Send + Sync + Unpin + 'static,
  A: Address + Send + Sync + Unpin + 'static,
  R: Runtime,
{
  type Runtime = R;
  type Id = I;
  type Address = A;
  fn meta(&self) -> &SnapshotMeta<Self::Id, Self::Address> {
    &self.meta.meta
  }
}

/// The [`SnapshotSink`] implementor for [`FileSnapshotStorage`].
pub struct FileSnapshotSink<I, A, R> {
  store: FileSnapshotStorage<I, A, R>,
  dir: PathBuf,

  no_sync: bool,

  file: BufWriter<ChecksumableWriter<File, crc32fast::Hasher>>,
  meta: FileSnapshotMeta<I, A>,
  closed: bool,
}

impl<I: Id, A: Address, R: Runtime> futures::io::AsyncWrite for FileSnapshotSink<I, A, R>
where
  I: Id + Send + Sync + Unpin + 'static,
  I::Error: Send + Sync + Unpin + 'static,
  A: Address + Send + Sync + Unpin + 'static,
  A::Error: Send + Sync + Unpin + 'static,
  R: Runtime,
{
  fn poll_write(
    mut self: Pin<&mut Self>,
    _cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    Poll::Ready(self.as_mut().file.write(buf))
  }

  fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(self.as_mut().file.flush())
  }

  fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    // Make sure close is idempotent
    if self.closed {
      return Poll::Ready(Ok(()));
    }

    self.as_mut().closed = true;

    // Close the open handles
    if let Err(e) = self.as_mut().finalize() {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to finalize snapshot");
      if let Err(e) = fs::remove_dir_all(&self.dir) {
        tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to delete temporary snapshot directory");
        return Poll::Ready(Err(e));
      }
      return Poll::Ready(Err(e));
    }

    // Write out the meta data
    if let Err(e) =
      FileSnapshotSink::<I, A, R>::write_meta(&self.dir, &self.meta, self.store.no_sync)
    {
      tracing::error!(target = "ruraft.snapshot.file", err = %e, "failed to write snapshot metadata");
      return Poll::Ready(Err(e));
    }

    // Move the directory into place
    let new_path = self.dir.to_str().unwrap().replace(TEMP_SUFFIX, "");
    if let Err(e) = fs::rename(&self.dir, &new_path) {
      tracing::error!(target = "ruraft.snapshot.file", old = %self.dir.display(), new = %new_path, err = %e, "failed to move snapshot directory into place");
      return Poll::Ready(Err(e));
    }

    if !self.no_sync {
      if let Some(parent) = self.dir.parent() {
        match fs::File::open(parent) {
          Ok(parent_fd) => {
            if let Err(e) = parent_fd.sync_all() {
              tracing::error!(target = "ruraft.snapshot.file", path = %parent.display(), err = %e, "failed syncing parent directory");
              return Poll::Ready(Err(e));
            }
          }
          Err(e) => {
            tracing::error!(target = "ruraft.snapshot.file", path = %parent.display(), err = %e, "failed to open snapshot parent directory");
            return Poll::Ready(Err(e));
          }
        }

        // Reap any old snapshots
        if let Err(e) = self.store.reap_snapshots() {
          return Poll::Ready(Err(e));
        }
      }
    }

    self.poll_flush(cx)
  }
}

impl<I, A, R> FileSnapshotSink<I, A, R>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  R: Runtime,
{
  fn finalize(&mut self) -> io::Result<()> {
    // Flush any remaining data
    self.file.flush()?;

    // sync to force fsync to disk
    if !self.no_sync {
      self.file.get_mut().inner_mut().sync_all()?;
    }

    // Get the file size
    let size = self.file.get_ref().inner().metadata()?.len();

    self.meta.meta.size = size;
    self.meta.crc = self.file.get_ref().checksum();
    Ok(())
  }

  fn write_meta<P: AsRef<Path>>(
    dir: &P,
    meta: &FileSnapshotMeta<I, A>,
    no_sync: bool,
  ) -> io::Result<()> {
    // Open the meta file
    let metapath = dir.as_ref().join(META_FILE_PATH.as_path());
    let mut fh = BufWriter::with_capacity(meta.encoded_len(), File::create(metapath)?);

    meta.encode_to_writer(&mut fh)?;
    fh.flush()?;

    if !no_sync {
      return fh.get_mut().sync_all();
    }

    Ok(())
  }
}

impl<I, A, R> SnapshotSink for FileSnapshotSink<I, A, R>
where
  I: Id + Send + Sync + Unpin + 'static,
  I::Error: Send + Sync + Unpin + 'static,
  A: Address + Send + Sync + Unpin + 'static,
  A::Error: Send + Sync + Unpin + 'static,
  R: Runtime,
{
  type Runtime = R;

  fn id(&self) -> SnapshotId {
    self.meta.meta.id()
  }

  async fn cancel(&mut self) -> io::Result<()> {
    // Make sure close is idempotent
    if self.closed {
      return Ok(());
    }

    self.closed = true;

    // Close the open handles
    self
      .finalize()
      .map_err(|e| {
        tracing::error!(target = "ruraft.snapshot.file", err=%e, "failed to finalize snapshot");
        e
      })
      .and_then(|_| fs::remove_dir_all(&self.dir))
  }

  async fn close(mut self) -> io::Result<()> {
    AsyncWriteExt::close(&mut self).await
  }
}

const CRC_SIZE: usize = mem::size_of::<u64>();

impl<I, A> Transformable for FileSnapshotMeta<I, A>
where
  I: Id + Send + Sync + 'static,
  <I as Transformable>::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  <A as Transformable>::Error: Send + Sync + 'static,
{
  type Error = <SnapshotMeta<I, A> as Transformable>::Error;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::EncodeBufferTooSmall);
    }

    self.meta.encode(dst)?;
    dst[encoded_len..encoded_len + CRC_SIZE].copy_from_slice(&self.crc.to_be_bytes());

    Ok(())
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
    self.meta.encode_to_writer(writer)?;
    writer.write_all(&self.crc.to_be_bytes())
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<()> {
    self.meta.encode_to_async_writer(writer).await?;
    writer.write_all(&self.crc.to_be_bytes()).await
  }

  fn encoded_len(&self) -> usize {
    self.meta.encoded_len() + CRC_SIZE
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    <SnapshotMeta<I, A> as Transformable>::decode(src).and_then(|(readed, meta)| {
      if src.len() < readed + CRC_SIZE {
        return Err(Self::Error::Corrupted);
      }
      let crc = u64::from_be_bytes(src[readed..readed + CRC_SIZE].try_into().unwrap());
      Ok((readed + CRC_SIZE, Self { meta, crc }))
    })
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <SnapshotMeta<I, A> as Transformable>::decode_from_reader(reader).and_then(|(readed, meta)| {
      let mut crc_buf = [0u8; CRC_SIZE];
      reader.read_exact(&mut crc_buf)?;
      let crc = u64::from_be_bytes(crc_buf);
      Ok((readed + CRC_SIZE, Self { meta, crc }))
    })
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    use futures::AsyncReadExt;

    let (readed, meta) =
      <SnapshotMeta<I, A> as Transformable>::decode_from_async_reader(reader).await?;

    let mut crc_buf = [0u8; CRC_SIZE];
    reader.read_exact(&mut crc_buf).await?;
    let crc = u64::from_be_bytes(crc_buf);
    Ok((readed + CRC_SIZE, Self { meta, crc }))
  }
}

/// Exports unit tests to let users test [`FileSnapshotStorage`] implementation if they want to
/// use their own [`agnostic::Runtime`] implementation.
#[cfg(feature = "test")]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests {
  use std::net::SocketAddr;

  use futures::{AsyncReadExt, AsyncWriteExt};
  use smol_str::SmolStr;

  use super::*;

  /// Test [`FileSnapshotStorage`].
  ///
  /// Description:
  /// - create snapshot and missing the parent dir
  pub async fn file_snapshot_storage_create_snapshot_missing_parent_dir<R: Runtime>() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("raft");
    fs::create_dir(&dir).unwrap();

    let snap =
      FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(FileSnapshotStorageOptions::new(&dir, 3))
        .await
        .unwrap();

    snap
      .create(SnapshotVersion::V1, 10, 3, Membership::__empty(), 0)
      .await
      .unwrap();
  }

  /// Test [`FileSnapshotStorage`].
  ///
  /// Description:
  /// - create snapshot
  pub async fn file_snapshot_storage_create_snapshot<R: Runtime>() {
    let parent = std::env::temp_dir();
    let dir = parent.as_path().join("raft");
    fs::create_dir(&dir).unwrap();
    scopeguard::defer!(fs::remove_dir_all(&dir).unwrap());
    let snap =
      FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(FileSnapshotStorageOptions::new(&dir, 3))
        .await
        .unwrap();

    // check no snapshots
    assert_eq!(snap.list().await.unwrap().len(), 0);

    // create a new sink
    let mut sink = snap
      .create(SnapshotVersion::V1, 10, 3, Membership::__single_server(), 2)
      .await
      .unwrap();

    // The sink is not done, should not be in a list!
    assert_eq!(snap.list().await.unwrap().len(), 0);

    // write to the sink
    sink.write_all(b"first\n").await.unwrap();

    sink.write_all(b"second\n").await.unwrap();

    // Done!
    sink.close().await.unwrap();

    // shold have a snapshot
    let snaps = snap.list().await.unwrap();
    assert_eq!(snaps.len(), 1);

    // check the latest
    let latest = &snaps[0];
    assert_eq!(latest.index, 10);
    assert_eq!(latest.term, 3);
    assert_eq!(latest.membership_index, 2);
    assert_eq!(latest.size, 13);

    // Read the snapshot
    let mut src = snap.open(&latest.id()).await.unwrap();

    // Read out everything
    let mut buf = Vec::new();
    src.read_to_end(&mut buf).await.unwrap();

    // Ensure a match
    assert_eq!(buf, b"first\nsecond\n");
  }

  /// Test [`FileSnapshotStorage`].
  ///
  /// Description:
  /// - create snapshot and cancel it
  pub async fn file_snapshot_storage_cancel_snapshot<R: Runtime>() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("raft");
    fs::create_dir(&dir).unwrap();

    let storage =
      FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(FileSnapshotStorageOptions::new(&dir, 3))
        .await
        .unwrap();

    let mut snap = storage
      .create(SnapshotVersion::V1, 10, 2, Membership::__empty(), 0)
      .await
      .unwrap();

    // Cancel the snapshot! Should delete
    snap.cancel().await.unwrap();

    // The sink is canceled, should not be in a list!
    assert_eq!(storage.list().await.unwrap().len(), 0);
  }

  /// Test [`FileSnapshotStorage`].
  ///
  /// Description:
  /// - create snapshot and retention
  pub async fn file_snapshot_storage_retention<R: Runtime>() {
    let parent = std::env::temp_dir();
    let dir = parent.as_path().join("raft");
    fs::create_dir(&dir).unwrap();
    scopeguard::defer!(fs::remove_dir_all(&dir).unwrap());

    let storage =
      FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(FileSnapshotStorageOptions::new(&dir, 2))
        .await
        .unwrap();

    for i in 10..15 {
      let sink = storage
        .create(
          SnapshotVersion::V1,
          i as u64,
          3,
          Membership::__large_membership(),
          0,
        )
        .await
        .unwrap();
      sink.close().await.unwrap();
    }

    // Should only have 2 listed!

    let snaps = storage.list().await.unwrap();
    assert_eq!(snaps.len(), 2);

    // check the latest
    assert_eq!(snaps[0].index, 14);
    assert_eq!(snaps[1].index, 13);
  }

  /// Test [`FileSnapshotStorage`].
  ///
  /// Description:
  /// - bad perm
  #[cfg(unix)]
  pub async fn file_snapshot_storage_bad_perm<R: Runtime>() {
    use std::os::unix::fs::PermissionsExt;
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("raft");
    fs::create_dir(&dir).unwrap();

    let dir2 = dir.join("badperm");
    fs::create_dir(&dir2).unwrap();
    let mut perm = std::fs::metadata(&dir2).unwrap().permissions();
    perm.set_mode(0o000);
    fs::set_permissions(&dir2, perm).unwrap();

    let Err(err) =
      FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(FileSnapshotStorageOptions::new(&dir2, 3))
        .await
    else {
      panic!("should fail to use dir with bad perms");
    };
    assert!(matches!(
      err,
      FileSnapshotStorageError::PathNotAccessible(_)
    ));
  }

  /// Test [`FileSnapshotStorage`].
  ///
  /// Description:
  /// - missing parent dir
  pub async fn file_snapshot_storage_missing_parent_dir<R: Runtime>() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("raft");
    fs::create_dir(&dir).unwrap();

    let dir2 = dir.join("raft");
    drop(parent);

    FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(FileSnapshotStorageOptions::new(&dir2, 3))
      .await
      .expect("should not fail when using non existing parent");
  }

  /// Test [`FileSnapshotStorage`].
  ///
  /// Description:
  /// - ordering
  pub async fn file_snapshot_storage_ordering<R: Runtime>() {
    let parent = std::env::temp_dir();
    let dir = parent.as_path().join("raft");
    fs::create_dir(&dir).unwrap();
    scopeguard::defer!(fs::remove_dir_all(&dir).unwrap());

    let storage =
      FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(FileSnapshotStorageOptions::new(&dir, 3))
        .await
        .unwrap();

    let sink = storage
      .create(
        SnapshotVersion::V1,
        130350,
        5,
        Membership::__sample_membership(),
        0,
      )
      .await
      .unwrap();
    sink.close().await.unwrap();

    let sink = storage
      .create(
        SnapshotVersion::V1,
        204917,
        36,
        Membership::__single_server(),
        0,
      )
      .await
      .unwrap();
    sink.close().await.unwrap();

    // Should only have 2 listed!
    let snaps = storage.list().await.unwrap();
    assert_eq!(snaps.len(), 2);

    // Check they are ordered
    assert_eq!(snaps[0].term, 36);
    assert_eq!(snaps[1].term, 5);
  }
}
