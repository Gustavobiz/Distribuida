package distribuida.common;

public class PeerInfo {
    public String nodeId;
    public String ip;
    public int port;

    public PeerInfo() {}

    public PeerInfo(String nodeId, String ip, int port) {
        this.nodeId = nodeId;
        this.ip = ip;
        this.port = port;
    }
}
