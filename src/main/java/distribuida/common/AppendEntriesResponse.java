package distribuida.common;

/**
 * Resposta para AppendEntries.
 */
public class AppendEntriesResponse {
    public int term;
    public boolean success;

    public AppendEntriesResponse(int term, boolean success) {
        this.term = term;
        this.success = success;
    }
}
