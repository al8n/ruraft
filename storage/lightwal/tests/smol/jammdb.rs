use super::*;

use ruraft_lightwal::jammdb::test::*;

unit_tests!(
  SmolRuntime => run(
    first_index,
    last_index,
    get_log,
    store_log,
    store_logs,
    remove_range,
    oldest_log,
    current_term,
    last_vote_term,
    last_vote_candidate,
  )
);
