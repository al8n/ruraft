use super::*;

use ruraft_lightwal::redb::test::*;

unit_tests!(
  TokioRuntime => run(
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
