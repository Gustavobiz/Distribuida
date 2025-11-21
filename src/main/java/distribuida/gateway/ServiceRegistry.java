package distribuida.gateway;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ServiceRegistry
 * ----------------
 * Mantém registro dos nós (A1, A2, B1...), seus IPs, portas,
 * último heartbeat e papel (LEADER/FOLLOWER).
 *
 * O Gateway usa esse registro para:
 *  - descobrir o líder
 *  - listar followers
 *  - enviar lista de peers para os nodes
 *  - monitorar falhas (timeout de heartbeat)
 */
public class ServiceRegistry {

    // Mapa: nodeId -> informações do nó
    private final Map<String, NodeInfo> registry = new ConcurrentHashMap<>();

    // Tempo máximo sem heartbeat antes de marcar o nó como morto
    private static final long HEARTBEAT_TIMEOUT_MS = 5000;

    /**
     * Estrutura com informações básicas do nó.
     * Enviada ao Gateway via REGISTER (UDP).
     */
    public static class NodeInfo {
        public final String nodeId;
        public final String ip;
        public final int port;
        public String role; // "LEADER" ou "FOLLOWER"
        public volatile long lastHeartbeatTime;
        public volatile boolean isActive;

        public NodeInfo(String nodeId, String ip, int port, String role) {
            this.nodeId = nodeId;
            this.ip = ip;
            this.port = port;
            this.role = role;
            this.lastHeartbeatTime = System.currentTimeMillis();
            this.isActive = true;
        }

        @Override
        public String toString() {
            return "NodeInfo{nodeId='" + nodeId + "', ip='" + ip +
                    "', port=" + port + ", role='" + role +
                    "', active=" + isActive + "}";
        }
    }

    // Singleton do Gateway
    private static final ServiceRegistry instance = new ServiceRegistry();

    private ServiceRegistry() {
        startCleanupTask();
    }

    public static ServiceRegistry getInstance() {
        return instance;
    }

    // ------------------------------------------------------------
    // REGISTRO / HEARTBEAT
    // ------------------------------------------------------------

    /** Registro inicial enviado pelo Node via UDP */
    public synchronized void registerNode(String nodeId, String ip, int port, String role) {
        NodeInfo info = new NodeInfo(nodeId, ip, port, role);
        registry.put(nodeId, info);
        System.out.println("[registry] registered: " + info);
    }

    /** Atualiza heartbeat vindo dos nodes */
    public void updateHeartbeat(String nodeId) {
        NodeInfo info = registry.get(nodeId);

        if (info != null) {
            info.lastHeartbeatTime = System.currentTimeMillis();
            if (!info.isActive) {
                System.out.println("[registry] node back ACTIVE: " + nodeId);
            }
            info.isActive = true;
        } else {
            System.out.println("[registry] heartbeat from UNKNOWN node: " + nodeId);
        }
    }

    // ------------------------------------------------------------
    // VERIFICAÇÃO DE TIMEOUT DE HEARTBEAT
    // ------------------------------------------------------------
    private void startCleanupTask() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(HEARTBEAT_TIMEOUT_MS);
                    checkHeartbeats();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }

    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        for (NodeInfo info : registry.values()) {
            if (now - info.lastHeartbeatTime > HEARTBEAT_TIMEOUT_MS) {
                if (info.isActive) {
                    info.isActive = false;
                    System.out.println("[registry] node marked INACTIVE: " + info.nodeId);
                }
            }
        }
    }

    // ------------------------------------------------------------
    // CONSULTAS
    // ------------------------------------------------------------

    /** Retorna o líder ativo atual */
    public Optional<NodeInfo> getActiveLeader() {
        return registry.values().stream()
                .filter(n -> n.isActive && "LEADER".equalsIgnoreCase(n.role))
                .findFirst();
    }

    /** Retorna qualquer réplica ativa (para READ) */
    public Optional<NodeInfo> getAnyActiveReplica() {
        return registry.values().stream()
                .filter(n -> n.isActive)
                .findAny();
    }

    /** Retorna lista de todos os nós ativos */
    public List<NodeInfo> getActiveNodes() {
        return registry.values().stream()
                .filter(n -> n.isActive)
                .collect(Collectors.toList());
    }

    /** Quantidade de nós ativos */
    public int countActiveNodes() {
        return (int) registry.values().stream()
                .filter(n -> n.isActive)
                .count();
    }

    /** Lista completa dos nós */
    public Collection<NodeInfo> listAllNodes() {
        return Collections.unmodifiableCollection(registry.values());
    }

    // ------------------------------------------------------------
    // JSON DE PEERS PARA ENVIAR AOS NODES
    // ------------------------------------------------------------

    /**
     * Retorna um JSON com todos os peers ativos, no formato:
     *
     * { "peers": [ { "nodeId":"A1", "ip":"127.0.1.1", "port":6000 }, ... ] }
     */
    public String buildPeersJson() {
        String peersArray = registry.values().stream()
                .filter(n -> n.isActive)
                .map(n -> String.format(
                        "{\"nodeId\":\"%s\",\"ip\":\"%s\",\"port\":%d}",
                        n.nodeId, n.ip, n.port))
                .collect(Collectors.joining(","));

        return "{ \"peers\": [" + peersArray + "] }";
    }
}
