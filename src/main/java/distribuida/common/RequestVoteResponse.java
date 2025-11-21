package distribuida.common;

public class RequestVoteResponse {
    public int term;
    public boolean voteGranted;

    public RequestVoteResponse() {}

    public RequestVoteResponse(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}
