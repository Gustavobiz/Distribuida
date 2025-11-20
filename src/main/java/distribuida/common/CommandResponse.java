package distribuida.common;

public class CommandResponse {
    // "OK" ou "ERROR"
    public String status;
    public String value;   // usado em READ
    public String message; // explicação (opcional)

    public CommandResponse() {}

    public CommandResponse(String status, String value, String message) {
        this.status = status;
        this.value = value;
        this.message = message;
    }

    @Override
    public String toString() {
        return "CommandResponse{" +
                "status='" + status + '\'' +
                ", value='" + value + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
