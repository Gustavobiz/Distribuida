package distribuida.common;

public class LogEntry {
    public int index;
    public int term;
    public String key;
    public String value;

    public LogEntry() {}

    public LogEntry(int index, int term, String key, String value) {
        this.index = index;
        this.term = term;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "LogEntry{index=" + index + ", term=" + term +
                ", key='" + key + "', value='" + value + "'}";
    }
}
