use std::sync::Arc;

use async_lock::Mutex;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use pyo3::{exceptions::PyIOError, prelude::*, types::PyBytes};
use pyo3_asyncio::async_std::*;
use smallvec::SmallVec;

use crate::INLINED_U8;

use super::*;

impl_source_sink!();
