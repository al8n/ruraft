/// Autopilot is the type to manage a running Raft instance.
///
/// Each Raft node in the cluster will have a corresponding [`Autopilot`] instance but
/// only 1 Autopilot instance should run at a time in the cluster. So when a node
/// gains Raft leadership the corresponding [`Autopilot`] instance should have it's
/// [`run`] method called. Then if leadership is lost that node should call the
/// [`stop`] method on the Autopilot instance.
pub struct Autopilot {

}