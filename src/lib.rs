use std::ops::Index;

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone, Copy)]
struct Term(u64);

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone, Copy)]
struct NodeID(u64);

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone, Copy)]
struct LogIndex(u64);

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone)]
struct Log<Entry> {
    entries: Vec<(Term, Entry)>,
}

impl<Entry> Index<LogIndex> for Log<Entry> {
    type Output = (Term, Entry);

    fn index(&self, index: LogIndex) -> &Self::Output {
        let LogIndex(i) = index;
        &self.entries[i as usize]
    }
}

impl<Entry> Log<Entry> {
    fn include_after<I: Iterator<Item = (Term, Entry)>>(
        &mut self,
        prev_index: LogIndex,
        mut entries: I,
    ) {
        let LogIndex(prev_index) = prev_index;
        let mut i = 1;
        'overwrite_existing_values: loop {
            if let Some((new_term, new_entry)) = entries.next() {
                match self.entries.get_mut(prev_index as usize + i) {
                    Some((ref mut term, ref mut entry)) => {
                        *term = new_term;
                        *entry = new_entry;
                        i += 1;
                    }
                    None => {
                        self.entries.extend(entries);
                        break 'overwrite_existing_values;
                    }
                }
            } else {
                break 'overwrite_existing_values;
            }
        }
    }
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct Config {
    id: NodeID,
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct RaftState<Entry> {
    /// Static configuration for this instance of the protocol.
    config: Config,
    /// Latest term server has seen. Initialized to 0 on first boot,
    /// increases monotonically.
    ///
    /// Must be persisted for recovery.
    current_term: Term,
    /// Candidate ID which received vote in the current term, None otherwise.
    ///
    /// Must be persisted for recovery.
    voted_for: Option<NodeID>,
    /// All of the log entries this server has seen.
    ///
    /// Must be persisted for recovery.
    log: Log<Entry>,
    /// Index of highest log entry known to be committed. Initialized to 0,
    /// increases monotonically,
    commit_index: LogIndex,
    /// Index of highest log entry applied to state machine. Initialized to 0,
    /// increases monotonically.
    last_applied: LogIndex,
    /// If you're the leader in the current term, you will have leader state as
    /// well.
    leader: Option<LeaderState>,
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct LeaderState {
    next_index: Vec<LogIndex>,
    match_index: Vec<LogIndex>,
}

struct AppendEntries<Entry> {
    /// Leader's term.
    term: Term,
    /// Current leader, so followers can redirect clients.
    leader: NodeID,
    /// Index of log entry immediately preceding new ones.
    previous_log_index: LogIndex,
    /// Term of previous_log_index entry.
    previous_log_term: Term,
    /// Log entries to store. Empty for heartbeat, may send more than one for
    /// efficiency.
    entries: Vec<(Term, Entry)>,
    /// Leader's commit_index.
    leader_commit: LogIndex,
}

struct AppendEntriesResponse {
    /// Current term, for leader to update itself.
    term: Term,
    /// Whethers or not the follower contained entry matching previous_log_index
    /// and previous_log_term.
    success: bool,
}

impl<Entry> RaftState<Entry> {
    fn append_entries(&mut self, append_entries: AppendEntries<Entry>) -> AppendEntriesResponse {
        if self.current_term > append_entries.term
            || self.log[append_entries.previous_log_index].0 != append_entries.previous_log_term
        {
            AppendEntriesResponse {
                term: self.current_term,
                success: false,
            }
        } else {
            self.current_term = append_entries.term;
            self.log.include_after(
                append_entries.previous_log_index,
                append_entries.entries.into_iter(),
            );
            if append_entries.leader_commit > self.commit_index {
                self.commit_index = append_entries.leader_commit;
            }
            AppendEntriesResponse {
                term: self.current_term,
                success: true,
            }
        }
    }
}

struct RequestVote {
    /// Candidate's term.
    term: Term,
    /// Candidate which is requesting vote.
    candidate_id: NodeID,
    /// Index of candidate's last log entry.
    last_log_index: LogIndex,
    /// Term of candidate's last log entry.
    last_log_term: Term,
}

struct RequestVoteResponse {
    /// Current term, for candidate to update itself.
    term: Term,
    /// Whether the candidate was granted the vote.
    vote_granted: bool,
}

impl<Entry> RaftState<Entry> {
    fn request_vote(&mut self, request_vote: RequestVote) -> RequestVoteResponse {
        if self.current_term > request_vote.term {
            RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            }
        } else if self.voted_for.is_none() || self.voted_for == Some(request_vote.candidate_id) {
            self.current_term = request_vote.term;
            self.voted_for = Some(request_vote.candidate_id);
            RequestVoteResponse {
                term: self.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            }
        }
    }
}
