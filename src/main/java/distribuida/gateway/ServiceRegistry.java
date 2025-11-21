package distribuida.gateway;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Registro dos nós (Leader / Followers) conhecido pelo Gateway.
 * Implementa Heartbeat com timeout para marcar nós como inativos.
 */
public class ServiceRegistry {

    // Mapa: nodeId -> informações do nó
    private final Map<String, NodeInfo> registry = new ConcurrentHashMap<>();

    // Tempo máximo sem heartbeat para considerar nó morto (ms)
    private static final long HEARTBEAT_TIMEOUT_MS = 5_000;

    public static class NodeInfo {
        public final String nodeId;
        public final String ip;
        public final int port;
        public String role; // "LEADER", "FOLLOWER" etc.
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
            return "NodeInfo{" +
                    "nodeId='" + nodeId + '\'' +
                    ", ip='" + ip + '\'' +
                    ", port=" + port +
                    ", role='" + role + '\'' +
                    ", lastHeartbeatTime=" + lastHeartbeatTime +
                    ", isActive=" + isActive +
                    '}';
        }
    }

    // Singleton
    private static final ServiceRegistry instance = new ServiceRegistry();

    private ServiceRegistry() {
        startCleanupTask();
    }

    public static ServiceRegistry getInstance() {
        return instance;
    }

    // Registro inicial do nó (startup)
    public synchronized void registerNode(String nodeId, String ip, int port, String role) {
        NodeInfo info = new NodeInfo(nodeId, ip, port, role);
        registry.put(nodeId, info);
        System.out.println("[registry] registered: " + info);
    }

    // Atualizar heartbeat (no recebe sinal de vida)
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

    // Thread periódica para verificar nós mortos
    private void startCleanupTask() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(HEARTBEAT_TIMEOUT_MS);
                    checkHeartbeats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    // Verifica se algum nó ultrapassou o timeout
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

    // Pegar Leader ativo (para WRITE)
    public Optional<NodeInfo> getActiveLeader() {
        return registry.values().stream()
                .filter(info -> "LEADER".equalsIgnoreCase(info.role) && info.isActive)
                .findFirst();
    }

    // Pegar Followers ativos (para READ / replicação)
    public List<NodeInfo> getActiveFollowers() {
        return registry.values().stream()
                .filter(info -> "FOLLOWER".equalsIgnoreCase(info.role) && info.isActive)
                .collect(Collectors.toList());
    }

    // Pegar qualquer réplica ativa (para READ simples)
    public Optional<NodeInfo> getAnyActiveReplica() {
        return registry.values().stream()
                .filter(info -> info.isActive)
                .findAny();
    }

    public Collection<NodeInfo> listAllNodes() {
        return Collections.unmodifiableCollection(registry.values());
    }

    // ------------------------------------------------------
    // RETORNAR TODOS OS NÓS ATIVOS
    // ------------------------------------------------------
    public List<NodeInfo> getActiveNodes() {
        return registry.values()
                .stream()
                .filter(n -> n.isActive)
                .toList();
    }

    // ------------------------------------------------------
    // CONTAR NÓS ATIVOS
    // ------------------------------------------------------
    public int countActiveNodes() {
        return (int) registry.values()
                .stream()
                .filter(n -> n.isActive)
                .count();
    }

    // ------------------------------------------------------
    // RETORNAR LISTA DE PEERS (para enviar ao Node)
    // ------------------------------------------------------
    public String toPeersJson() {
        var list = registry.values()
                .stream()
                .filter(n -> n.isActive)
                .map(n -> String.format(
                        "{\"nodeId\":\"%s\",\"ip\":\"%s\",\"port\":%d}",
                        n.nodeId, n.ip, n.port
                ))
                .collect(Collectors.joining(","));

        return "[" + list + "]";
    }
}
