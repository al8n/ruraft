use super::*;
use crate::IntoSupportedRuntime;
use futures::StreamExt;

#[macro_export]
macro_rules! leadership_watcher {
  ($($rt: literal), +$(,)?) => {
    $(
      paste::paste! {
        /// A stream which can be used to receive leadership changes.
        ///
        /// - `true` indicates the node becomes the leader.
        /// - `false` indicates the node is not the leader anymore.
        #[derive(Clone)]
        #[cfg(feature = $rt)]
        #[pyclass(name = "LeadershipWatcher")]
        pub struct [< $rt:camel LeadershipWatcher >](pub(crate) ruraft_core::LeadershipWatcher);

        #[cfg(feature = $rt)]
        #[pymethods]
        impl [< $rt:camel LeadershipWatcher >] {
          pub fn next<'a>(&'a self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a PyAny> {
            let mut this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              futures::pin_mut!(this);
              Ok(this.next().await)
            })
          }

          fn __aiter__(slf: ::pyo3::PyRef<'_, Self>) -> ::pyo3::PyRef<Self> {
            slf
          }

          fn __anext__(slf: ::pyo3::PyRefMut<'_, Self>) -> ::pyo3::PyResult<Option<::pyo3::PyObject>> {
            let watcher = slf.0.clone();
            let fut = ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(slf.py(), async move {
              futures::pin_mut!(watcher);
              match watcher.next().await {
                Some(val) => Ok(val),
                None => Err(::pyo3::exceptions::PyStopAsyncIteration::new_err("stream exhausted")),
              }
            })?;

            Ok(Some(fut.into()))
          }
        }
      }
    )*
  };
}

#[macro_export]
macro_rules! wrap_fut {
  ($rt:literal::$ty:literal) => {
    paste::paste! {
      #[cfg(feature = $rt)]
      #[::pyo3::pyclass(name = $ty)]
      pub struct [< $rt:camel $ty >]([< $ty >] < ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >] >);

      #[cfg(feature = $rt)]
      impl From<ruraft_core::[< $ty >] <$crate::fsm::FinateStateMachine<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, $crate::RaftStorage<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, $crate::RaftTransport<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime > ]>>> for [< $rt:camel $ty >] {
        fn from(val: ruraft_core::[< $ty >] <$crate::fsm::FinateStateMachine<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, $crate::RaftStorage<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, $crate::RaftTransport<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime > ]>>) -> Self {
          Self([< $ty >](Some(val)))
        }
      }

      #[cfg(feature = $rt)]
      #[::pyo3::pymethods]
      impl [< $rt:camel $ty >] {
        /// Wait the future to be finished and get the response. This function can only be invoked once.
        /// Otherwise, the `wait` method will lead to exceptions.
        pub fn wait<'a>(&'a mut self, py: ::pyo3::Python<'a>) -> ::pyo3::PyResult<&'a ::pyo3::PyAny> {
          self.0.wait(py)
        }
      }
    }
  };
}

#[macro_export]
macro_rules! state_machine_futs {
  ($($ty:literal), +$(,)?) => {
    $(
      paste::paste! {
        pub struct [< $ty >]<R>(Option<ruraft_core::[< $ty >] <$crate::fsm::FinateStateMachine<R>, $crate::RaftStorage<R>, $crate::RaftTransport<R>>>)
        where
          R: $crate::IntoSupportedRuntime,
          <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
          $crate::storage::SnapshotSource<R>: $crate::IntoPython,
          $crate::fsm::FinateStateMachineSnapshot<R>: $crate::FromPython<Source = <$crate::fsm::FinateStateMachineSnapshot<R> as $crate::IntoPython>::Target> + $crate::IntoPython;

        impl<R> [< $ty >]<R>
        where
          R: $crate::IntoSupportedRuntime,
          <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
          $crate::storage::SnapshotSource<R>: $crate::IntoPython,
          $crate::fsm::FinateStateMachineSnapshot<R>: $crate::FromPython<Source = <$crate::fsm::FinateStateMachineSnapshot<R> as $crate::IntoPython>::Target> + $crate::IntoPython,
        {
          pub fn wait<'a>(&'a mut self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            match self.0.take() {
              Some(f) => {
                R::into_supported().future_into_py(py, async move {
                  f.await.map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))
                })
              }
              None => {
                Err(pyo3::PyErr::new::<pyo3::exceptions::PyTypeError, _>(concat!(stringify!($ty), ".wait() have been consumed.")))
              }
            }
          }
        }
      }

      wrap_fut!("tokio" :: $ty);
      wrap_fut!("async-std" :: $ty);
    )*
  };
}

macro_rules! register {
  ($($rt: literal), +$(,)?) => {
    $(
      paste::paste! {
        #[cfg(feature = $rt)]
        pub fn [< register_ $rt:snake >](m: &PyModule) -> pyo3::PyResult<()> {
          m.add_class::<[< $rt:camel ApplyFuture >]>()?;
          m.add_class::<[< $rt:camel BarrierFuture >]>()?;
          m.add_class::<[< $rt:camel MembershipChangeFuture >]>()?;
          m.add_class::<[< $rt:camel VerifyFuture >]>()?;
          m.add_class::<[< $rt:camel LeadershipTransferFuture >]>()?;
          m.add_class::<[< $rt:camel SnapshotFuture >]>()?;
          m.add_class::<[< $rt:camel LeadershipWatcher >]>()?;
          m.add_class::<crate::storage:: [< $rt:camel SnapshotSource >]>()?;

          Ok(())
        }
      }
    )*
  };
}

pub struct SnapshotFuture<R>(
  Option<
    ruraft_core::SnapshotFuture<
      crate::fsm::FinateStateMachine<R>,
      crate::storage::RaftStorage<R>,
      crate::RaftTransport<R>,
    >,
  >,
)
where
  R: crate::IntoSupportedRuntime,
  <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
  crate::storage::SnapshotSource<R>: crate::IntoPython,
  crate::fsm::FinateStateMachineSnapshot<R>: crate::FromPython<
      Source = <crate::fsm::FinateStateMachineSnapshot<R> as crate::IntoPython>::Target,
    > + crate::IntoPython;

impl<R> SnapshotFuture<R>
where
  R: crate::IntoSupportedRuntime,
  crate::storage::SnapshotSource<R>: crate::IntoPython,
  <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
  crate::fsm::FinateStateMachineSnapshot<R>: crate::FromPython<
      Source = <crate::fsm::FinateStateMachineSnapshot<R> as crate::IntoPython>::Target,
    > + crate::IntoPython,
{
  pub fn wait<'a>(&'a mut self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a PyAny> {
    match self.0.take() {
      Some(f) => R::into_supported().future_into_py(py, async move {
        match f.await {
          Ok(res) => Ok(crate::IntoPython::into_python(
            crate::storage::SnapshotSource::from(res),
          )),
          Err(e) => Err(pyo3::PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            e.to_string(),
          )),
        }
      }),
      None => Err(pyo3::PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "SnapshotFuture.wait() have been consumed.",
      )),
    }
  }
}

wrap_fut!("tokio" :: "SnapshotFuture");
wrap_fut!("async-std" :: "SnapshotFuture");

state_machine_futs!(
  "ApplyFuture",
  "BarrierFuture",
  "MembershipChangeFuture",
  "VerifyFuture",
  "LeadershipTransferFuture",
);

leadership_watcher!("tokio", "async-std");
register!("tokio", "async-std");
