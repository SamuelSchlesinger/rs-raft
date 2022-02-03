#![feature(never_type)]

use std::{fmt::Debug, ops::Index, time::Duration};

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone, Copy)]
struct Term(u64);

impl Term {
    fn increment(&mut self) {
        let Term(i) = *self;
        *self = Term(i + 1);
    }
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone, Copy)]
struct NodeID(u64);

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone, Copy)]
struct LogIndex(u64);

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone)]
struct Log<Entry> {
    /// Entries in the log.
    entries: Vec<(Term, Entry)>,
    /// Index of highest log entry known to be committed. Initialized to 0,
    /// increases monotonically,
    commit_index: LogIndex,
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
        leader_commit: LogIndex,
        mut entries: I,
    ) {
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit;
        }
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
    /// Index of highest log entry applied to state machine. Initialized to 0,
    /// increases monotonically.
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
                append_entries.leader_commit,
                append_entries.entries.into_iter(),
            );
            if append_entries.leader_commit > self.log.commit_index {
                self.log.commit_index = append_entries.leader_commit;
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

enum Rpc<Entry> {
    AppendEntries(AppendEntries<Entry>),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
}

trait Context<Entry> {
    type RecvError: Debug;
    type RecoveryError: Debug;

    fn send(&mut self, rpc: Rpc<Entry>, to: NodeID);
    fn broadcast(&mut self, rpc: Rpc<Entry>);
    fn receive(&mut self, timeout: Duration) -> Result<Rpc<Entry>, Self::RecvError>;
    fn save(&mut self, state: RaftState<Entry>);
    fn recover(&mut self) -> Result<RaftState<Entry>, Self::RecoveryError>;
    fn timeout(&self) -> Duration;
}

#[derive(Debug, Clone, Copy)]
enum ProtocolRole {
    Leader,
    Candidate,
    Follower,
}

#[derive(Debug)]
enum RaftError<Ctx: Context<Entry>, Entry> {
    RecvError(<Ctx as Context<Entry>>::RecvError),
    RecoveryError(<Ctx as Context<Entry>>::RecoveryError),
}

fn run<Entry, Ctx: Context<Entry>>(mut ctx: Ctx) -> Result<!, RaftError<Ctx, Entry>> {
    let mut state = ctx.recover().map_err(RaftError::RecoveryError)?;
    let mut role = ProtocolRole::Follower;
    loop {
        match role {
            ProtocolRole::Leader => todo!(),
            ProtocolRole::Candidate => todo!(),
            ProtocolRole::Follower => {
                // Respond to RPCs from candidates and leaders, converting to
                // candidate if we don't get a message.
                match ctx.receive(ctx.timeout()) {
                    Err(_recv_err) => {
                        // Didn't receive a message, converting to candidate.
                        role = ProtocolRole::Candidate;
                        // Increment our current term so others will vote for us.
                        state.current_term.increment();
                        // TODO
                    }
                    Ok(msg) => match msg {
                        Rpc::AppendEntries(_) => todo!(),
                        Rpc::AppendEntriesResponse(_) => todo!(),
                        Rpc::RequestVote(_) => todo!(),
                        Rpc::RequestVoteResponse(_) => todo!(),
                    },
                }
            }
        }
    }
}
