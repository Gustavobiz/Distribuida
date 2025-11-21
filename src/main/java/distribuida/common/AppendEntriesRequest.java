package distribuida.common;

import java.util.List;

/**
 * Mensagem RAFT: AppendEntries
 * Usada tanto para heartbeat quanto para replicação real.
 */
public class AppendEntriesRequest {
    public int term;
    public String leaderId;

    public int prevLogIndex;
    public int prevLogTerm;

    public int leaderCommit;

    public String targetNodeId; // usado pelo Gateway para a entrega

    public List<LogEntry> entries;
}
