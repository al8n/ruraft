use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use pyo3::{exceptions::PyIOError, prelude::*, types::PyBytes};
use pyo3_asyncio::tokio::*;
use smallvec::SmallVec;

use crate::{FearlessCell, INLINED_U8};

use super::*;

impl_source_sink!();
