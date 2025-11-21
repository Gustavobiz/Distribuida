package distribuida.common;

public class RequestVoteRequest {
    public int term;
    public String candidateId;
    public int lastLogIndex;
    public int lastLogTerm;

    public RequestVoteRequest() {}

    public RequestVoteRequest(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
