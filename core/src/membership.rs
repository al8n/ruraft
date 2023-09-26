use serde::{Deserialize, Serialize};

/// Tracks which servers are in the cluster, and whether they have
/// votes. This should include the local server, if it's a member of the cluster.
/// The servers are listed no particular order, but each should only appear once.
/// These entries are appended to the log during membership changes.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Membership {}
