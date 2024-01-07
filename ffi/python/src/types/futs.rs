use super::*;
use futures::StreamExt;
use crate::IntoSupportedRuntime;

macro_rules! leadership_watcher {
  ($($rt: literal), +$(,)?) => {
    $(
      paste::paste! {
        /// A stream which can be used to receive leadership changes.
        ///
        /// - `true` indicates the node becomes the leader.
        /// - `false` indicates the node is not the leader anymore.
        #[derive(Clone)]
        #[pyclass(name = "LeadershipWatcher")]
        pub struct [< $rt:camel LeadershipWatcher >](pub(crate) ruraft_core::LeadershipWatcher);

        #[pymethods]
        impl [< $rt:camel LeadershipWatcher >] {
          pub fn next<'a>(&'a self, py: Python<'a>) -> PyResult<&'a PyAny> {
            let mut this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              futures::pin_mut!(this);
              Ok(this.next().await)
            })
          }
        }
      }
    )*
  };
}

macro_rules! wrap_fut {
  ($rt:literal::$ty:literal) => {
    paste::paste! {
      #[cfg(feature = $rt)]
      #[::pyo3::pyclass(name = $ty)]
      pub struct [< $rt:camel $ty >]([< $ty >] < ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >] >);

      #[cfg(feature = $rt)]
      impl From<ruraft_core::[< $ty >] <crate::fsm::FinateStateMachine<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, crate::RaftStorage<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, crate::RaftTransport<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime > ]>>> for [< $rt:camel $ty >] {
        fn from(val: ruraft_core::[< $ty >] <crate::fsm::FinateStateMachine<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, crate::RaftStorage<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>, crate::RaftTransport<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime > ]>>) -> Self {
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

macro_rules! state_machine_futs {
  ($($ty:literal), +$(,)?) => {
    $(
      paste::paste! {
        pub struct [< $ty >]<R>(Option<ruraft_core::[< $ty >] <crate::fsm::FinateStateMachine<R>, crate::RaftStorage<R>, crate::RaftTransport<R>>>)
        where
          R: crate::IntoSupportedRuntime,
          <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
          crate::storage::snapshot::SnapshotSink<R>: crate::IntoPython,
          crate::storage::snapshot::SnapshotSource<R>: crate::IntoPython,
          crate::fsm::FinateStateMachineSnapshot<R>: crate::FromPython<Source = <crate::fsm::FinateStateMachineSnapshot<R> as crate::IntoPython>::Target> + crate::IntoPython;

        impl<R> [< $ty >]<R>
        where
          R: crate::IntoSupportedRuntime,
          <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
          crate::storage::snapshot::SnapshotSink<R>: crate::IntoPython,
          crate::storage::snapshot::SnapshotSource<R>: crate::IntoPython,
          crate::fsm::FinateStateMachineSnapshot<R>: crate::FromPython<Source = <crate::fsm::FinateStateMachineSnapshot<R> as crate::IntoPython>::Target> + crate::IntoPython,
        {
          pub fn wait<'a>(&'a mut self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            match self.0.take() {
              Some(f) => {
                R::into_supported().future_into_py(py, async move {
                  f.await.map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))
                })
              }
              None => {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(concat!(stringify!($ty), ".wait() have been consumed.")))
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
        pub fn [< register_ $rt:snake >](m: &PyModule) -> PyResult<()> {
          let submodule = PyModule::new(m.py(), stringify!([< $rt:snake >]))?;

          submodule.add_class::<[< $rt:camel ApplyFuture >]>()?;
          submodule.add_class::<[< $rt:camel BarrierFuture >]>()?;
          submodule.add_class::<[< $rt:camel MembershipChangeFuture >]>()?;
          submodule.add_class::<[< $rt:camel VerifyFuture >]>()?;
          submodule.add_class::<[< $rt:camel LeadershipTransferFuture >]>()?;
          submodule.add_class::<[< $rt:camel SnapshotFuture >]>()?;
          submodule.add_class::<[< $rt:camel LeadershipWatcher >]>()?;

          m.add_submodule(submodule)?;
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
      crate::RaftStorage<R>,
      crate::RaftTransport<R>,
    >,
  >,
)
where
  R: crate::IntoSupportedRuntime,
  <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
  crate::storage::snapshot::SnapshotSink<R>: crate::IntoPython,
  crate::storage::snapshot::SnapshotSource<R>: crate::IntoPython,
  crate::fsm::FinateStateMachineSnapshot<R>: crate::FromPython<
      Source = <crate::fsm::FinateStateMachineSnapshot<R> as crate::IntoPython>::Target,
    > + crate::IntoPython;

impl<R> SnapshotFuture<R>
where
  R: crate::IntoSupportedRuntime,
  <<R as agnostic::Runtime>::Sleep as futures::Future>::Output: Send,
  crate::storage::snapshot::SnapshotSink<R>: crate::IntoPython,
  crate::storage::snapshot::SnapshotSource<R>: crate::IntoPython,
  crate::fsm::FinateStateMachineSnapshot<R>: crate::FromPython<
      Source = <crate::fsm::FinateStateMachineSnapshot<R> as crate::IntoPython>::Target,
    > + crate::IntoPython,
{
  pub fn wait<'a>(&'a mut self, py: Python<'a>) -> PyResult<&'a PyAny> {
    use ruraft_core::storage::SnapshotSource;

    match self.0.take() {
      Some(f) => R::into_supported().future_into_py(py, async move {
        match f.await {
          Ok(res) => match res.await {
            Ok(res) => {
              let meta = res.meta().clone();
              let cell = crate::FearlessCell::new(Box::new(res) as Box<_>);
              Ok(crate::IntoPython::into_python(
                crate::storage::snapshot::SnapshotSource::new(meta, cell),
              ))
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
              e.to_string(),
            )),
          },
          Err(e) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            e.to_string(),
          )),
        }
      }),
      None => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
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
