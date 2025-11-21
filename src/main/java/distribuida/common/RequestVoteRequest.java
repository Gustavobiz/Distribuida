package distribuida.common;

/**
 * Mensagem RAFT: RequestVote
 * Enviada do Candidate → Gateway → Node (via HTTP)
 */
public class RequestVoteRequest {
    public int term;
    public String candidateId;

    public int lastLogIndex;
    public int lastLogTerm;

    public String targetNodeId; // usado pelo Gateway para saber o destino
}
