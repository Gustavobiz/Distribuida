package distribuida.common;

public class CommandRequest {
    // "WRITE" ou "READ" (pode usar também "SET"/"GET")
    public String type;
    public String key;
    public String value;

    // construtor vazio para o Gson
    public CommandRequest() {}

    public CommandRequest(String type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "CommandRequest{" +
                "type='" + type + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
