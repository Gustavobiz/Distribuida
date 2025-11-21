package distribuida.node;

import com.google.gson.Gson;
import distribuida.common.CommandRequest;
import distribuida.common.CommandResponse;
import distribuida.common.LogEntry;
import distribuida.common.NodeRole;
import distribuida.common.RequestVoteRequest;
import distribuida.common.RequestVoteResponse;
import distribuida.common.PeerInfo;


import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * NodeMain completo — RAFT simplificado + Key/Value + Replicação + Eleição + Heartbeat.
 * Versão pronta para cluster A1 (Leader), A2/B1 (Followers).
 */
public class NodeMain {

    // --------------------------
    // PROCESS PARAMETERS
    // --------------------------
    private static String nodeId;
    private static String roleArg; // "LEADER"/"FOLLOWER" inicial (mas RAFT pode mudar)
    private static String gatewayHost;
    private static int gatewayRegPort; // 8000 (UDP)
    private static int localPort;      // TCP server port

    // --------------------------
    // RAFT STATE
    // --------------------------
    private static NodeRole nodeRole = NodeRole.FOLLOWER;

    private static int currentTerm = 0;
    private static String votedFor = null;

    private static final List<LogEntry> logEntries = new ArrayList<>();

    private static int commitIndex = 0;
    private static int lastApplied = 0;

    private static long nextElectionTime;
    private static final Random rand = new Random();
private static final List<PeerInfo> peers = new ArrayList<>();

    // --------------------------
    // KEY/VALUE STATE MACHINE
    // --------------------------
    private static final Map<String, String> store = new ConcurrentHashMap<>();

    // --------------------------
    // TRANSPORT
    // --------------------------
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {

        System.out.println("ARGS RECEIVED:");
        for (String a : args) System.out.println(" > " + a);

        // ----------------------------
        // PARSE DOS ARGUMENTOS
        // ----------------------------
        for (String arg : args) {
            if (arg.startsWith("--nodeId=")) nodeId = arg.substring("--nodeId=".length());
            else if (arg.startsWith("--role=")) roleArg = arg.substring("--role=".length());
            else if (arg.startsWith("--gatewayHost=")) gatewayHost = arg.substring("--gatewayHost=".length());
            else if (arg.startsWith("--gatewayRegPort=")) gatewayRegPort = Integer.parseInt(arg.substring("--gatewayRegPort=".length()));
            else if (arg.startsWith("--port=")) localPort = Integer.parseInt(arg.substring("--port=".length()));
        }

        if (nodeId == null || roleArg == null || gatewayHost == null || gatewayRegPort == 0 || localPort == 0) {
            System.out.println("Uso correto:");
            System.out.println("java NodeMain --nodeId=A1 --role=LEADER --port=5001 --gatewayHost=localhost --gatewayRegPort=8000");
            return;
        }

        nodeRole = roleArg.equalsIgnoreCase("LEADER") ? NodeRole.LEADER : NodeRole.FOLLOWER;

        System.out.println("[node] Starting nodeId=" + nodeId +
                " role=" + nodeRole +
                " localPort=" + localPort +
                " gateway=" + gatewayHost + ":" + gatewayRegPort);

        // ----------------------------
        // 1. Registrar no gateway
        // ----------------------------
        sendRegister();

        // ----------------------------
        // 2. HEARTBEAT para o Gateway (monitoramento)
        // ----------------------------
        startHeartbeatThread();

        // ----------------------------
        // 3. RAFT election timer
        // ----------------------------
        resetElectionTimeout();
        startElectionThread();

        // ----------------------------
        // 4. TCP server local para RECEIVE AppendEntries / commands
        // ----------------------------
        startLocalTcpServer();

        System.out.println("[node] Node ON");
    }

    // ======================================================
    // ========== REGISTRO & HEARTBEAT ======================
    // ======================================================

    private static void sendRegister() {
        try (DatagramSocket socket = new DatagramSocket()) {

            String ip = InetAddress.getLocalHost().getHostAddress();
            String msg = "REGISTER " + nodeId + " " + ip + " " + localPort + " " + roleArg;

            byte[] data = msg.getBytes(StandardCharsets.UTF_8);

            DatagramPacket packet = new DatagramPacket(
                    data, data.length,
                    InetAddress.getByName(gatewayHost),
                    gatewayRegPort
            );

socket.send(packet);

// AGORA LER A RESPOSTA DO GATEWAY
byte[] buf = new byte[8192];
DatagramPacket resp = new DatagramPacket(buf, buf.length);
socket.receive(resp);

String json = new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8);
System.out.println("[node] REGISTER response → " + json);

// FAZER PARSE DOS PEERS
PeerInfoResponse peerResp = gson.fromJson(json, PeerInfoResponse.class);
if (peerResp != null && peerResp.peers != null) {
    peers.clear();
    peers.addAll(peerResp.peers);
    System.out.println("[node] Peers atualizados: " + peers.size());
}


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startHeartbeatThread() {
        Thread t = new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket()) {

                while (true) {
                    String msg = "HEARTBEAT " + nodeId;

                    byte[] data = msg.getBytes(StandardCharsets.UTF_8);

                    DatagramPacket p = new DatagramPacket(
                            data, data.length,
                            InetAddress.getByName(gatewayHost),
                            gatewayRegPort
                    );

                    socket.send(p);
                    TimeUnit.SECONDS.sleep(2);
                }

            } catch (Exception e) {
                System.err.println("[node] heartbeat error: " + e.getMessage());
            }
        });

        t.setDaemon(true);
        t.start();
        System.out.println("[node] Heartbeat thread ON");
    }

    // ======================================================
    // ================ RAFT ELECTION =======================
    // ======================================================

    private static void resetElectionTimeout() {
        nextElectionTime = System.currentTimeMillis() + (150 + rand.nextInt(150));
    }

    private static void startElectionThread() {
        Thread t = new Thread(() -> {
            while (true) {
                try { Thread.sleep(50); } catch (Exception ignored) {}

                long now = System.currentTimeMillis();

                // Se não for líder e timeout estourar → vira candidato
                if (nodeRole != NodeRole.LEADER && now >= nextElectionTime) {
                    startElection();
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }

private static void startElection() {

    currentTerm++;
    nodeRole = NodeRole.CANDIDATE;
    votedFor = nodeId;

    System.out.println("[RAFT] " + nodeId + " virou CANDIDATE no term " + currentTerm);

    int lastLogIndex = logEntries.size();
    int lastLogTerm = lastLogIndex == 0 ? 0 : logEntries.get(lastLogIndex - 1).term;

    int votes = 1; // votou em si mesmo

    resetElectionTimeout();

List<PeerInfo> nodes = peers;


    RequestVoteRequest voteReq = new RequestVoteRequest(
            currentTerm, nodeId, lastLogIndex, lastLogTerm
    );

    for (var n : nodes) {
        if (n.nodeId.equals(nodeId)) continue; // não vota em si

        try {
            RequestVoteResponse resp = sendRequestVote(n.ip, n.port, voteReq);

            if (resp.voteGranted) votes++;
        } catch (Exception ignored) {}
    }

    int quorum = (nodes.size() / 2) + 1;

    if (votes >= quorum) {
        nodeRole = NodeRole.LEADER;
        System.out.println("[RAFT] " + nodeId + " foi ELEITO líder no term " + currentTerm +
                " com " + votes + " votos");
        startLeaderHeartbeatThread();
    } else {
        nodeRole = NodeRole.FOLLOWER;
    }
}


    private static void startLeaderHeartbeatThread() {
        Thread t = new Thread(() -> {
            while (nodeRole == NodeRole.LEADER) {
                try {
                    sendAppendEntriesHeartbeat();
                    Thread.sleep(120);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }

    private static void sendAppendEntriesHeartbeat() {
        // Por enquanto só loga; integração com gateway/followers pode ser feita depois
        System.out.println("[RAFT] Leader " + nodeId + " enviando AppendEntries vazio (heartbeat)...");
    }

    // ======================================================
    // ================ TCP SERVER (RECEIVER) ===============
    // ======================================================

    private static void startLocalTcpServer() {

        System.out.println("[node] Iniciando thread do servidor TCP...");

        Thread t = new Thread(() -> {
            System.out.println("[node] Thread do servidor TCP iniciada.");

            try {
                System.out.println("[node] Tentando abrir a porta TCP " + localPort + "...");

                ServerSocket serverSocket = new ServerSocket(localPort);

                System.out.println("[node] Servidor TCP ATIVO na porta " + localPort);

                while (true) {
                    System.out.println("[node] Aguardando conexão TCP...");
                    Socket client = serverSocket.accept();
                    System.out.println("[node] conexão TCP de " + client.getRemoteSocketAddress());

                    new Thread(() -> handleClient(client)).start();
                }

            } catch (Exception e) {
                System.err.println("[ERRO] Não foi possível iniciar servidor TCP:");
                e.printStackTrace();
            }
        });

        t.start(); // não daemon
        System.out.println("[node] Thread TCP iniciada (startLocalTcpServer)");
    }

    private static RequestVoteResponse sendRequestVote(String host, int port, RequestVoteRequest req) {
    try (Socket socket = new Socket(host, port);
         BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
         BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

        out.write(gson.toJson(req));
        out.newLine();
        out.flush();

        String resp = in.readLine();
        return gson.fromJson(resp, RequestVoteResponse.class);

    } catch (Exception e) {
        return new RequestVoteResponse(currentTerm, false);
    }
}

private static RequestVoteResponse handleRequestVote(RequestVoteRequest req) {

    if (req.term < currentTerm) {
        return new RequestVoteResponse(currentTerm, false);
    }

    if (req.term > currentTerm) {
        currentTerm = req.term;
        nodeRole = NodeRole.FOLLOWER;
        votedFor = null;
    }

    boolean voteGranted = false;

    boolean votedForNullOrCandidate =
            votedFor == null || votedFor.equals(req.candidateId);

    if (votedForNullOrCandidate) {
        votedFor = req.candidateId;
        voteGranted = true;
        resetElectionTimeout();
        System.out.println("[RAFT] " + nodeId + " votou para " + req.candidateId + " no term " + currentTerm);
    }

    return new RequestVoteResponse(currentTerm, voteGranted);
}
static class PeerInfoResponse {
    List<PeerInfo> peers;
}


    private static void handleClient(Socket client) {
        String remote = client.getRemoteSocketAddress().toString();

        try (BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String line;
            while ((line = in.readLine()) != null) {

                System.out.println("[node] recebido de " + remote + ": " + line);

                // --------------------
                // Interpretar comando
                // --------------------
                if (line.contains("\"candidateId\"") && line.contains("\"lastLogIndex\"")) {
    // requestVote
    RequestVoteRequest voteReq = gson.fromJson(line, RequestVoteRequest.class);
    RequestVoteResponse resp = handleRequestVote(voteReq);
    out.println(gson.toJson(resp));
    continue;
}


                if (line.contains("\"type\":\"APPEND\"")) {
                    // AppendEntries (simples)
                    CommandRequest req = gson.fromJson(line, CommandRequest.class);
                    CommandResponse resp = handleAppendEntries(req);
                    out.println(gson.toJson(resp));
                    continue;
                }

                // Comando normal WRITE/READ
                CommandResponse resp = processClientCommand(line);
                out.println(gson.toJson(resp));
            }

        } catch (Exception e) {
            System.err.println("[node] erro com cliente " + remote + ": " + e.getMessage());
        }
    }

    // ======================================================
    // ============== PROCESSAMENTO DE COMANDOS =============
    // ======================================================

    private static CommandResponse processClientCommand(String line) {

        CommandRequest req;
        try {
            req = gson.fromJson(line, CommandRequest.class);
        } catch (Exception e) {
            return new CommandResponse("ERROR", null, "JSON inválido: " + e.getMessage());
        }

        if (req == null || req.type == null) {
            return new CommandResponse("ERROR", null, "Requisição inválida");
        }

        switch (req.type.toUpperCase()) {

            case "WRITE":
            case "SET":
                return processWrite(req);

            case "READ":
            case "GET":
                return processRead(req);

            default:
                return new CommandResponse("ERROR", null, "Tipo desconhecido: " + req.type);
        }
    }

    private static CommandResponse processWrite(CommandRequest req) {

        if (nodeRole != NodeRole.LEADER) {
            return new CommandResponse("ERROR", null, "Nó não é LEADER");
        }

        int newIndex = logEntries.size() + 1;

        LogEntry entry = new LogEntry(newIndex, currentTerm, req.key, req.value);
        logEntries.add(entry);

        System.out.println("[RAFT] Leader recebeu WRITE → nova logEntry=" + entry);

        // TODO: replicar para followers
        return new CommandResponse("OK", null, "Valor gravado; replicação pendente");
    }

    private static CommandResponse processRead(CommandRequest req) {
        String value = store.get(req.key);
        if (value == null) {
            return new CommandResponse("ERROR", null, "Chave não encontrada");
        }
        return new CommandResponse("OK", value, "Valor lido com sucesso");
    }

    // ======================================================
    // ================ AppendEntries (RAFT) ================
    // ======================================================

    private static CommandResponse handleAppendEntries(CommandRequest req) {

        // Aqui vamos considerar que qualquer APPEND recebido atualiza o timeout
        // e mantém o nó como FOLLOWER, sem usar req.term/index (que não existem no CommandRequest).
        nodeRole = NodeRole.FOLLOWER;
        resetElectionTimeout();

        // Se vier key/value, tratamos como replicação de entrada de log
        if (req.key != null) {
            int newIndex = logEntries.size() + 1;
            LogEntry e = new LogEntry(newIndex, currentTerm, req.key, req.value);
            logEntries.add(e);
            store.put(req.key, req.value); // aplica na máquina de estado

            System.out.println("[RAFT] FOLLOWER aplicou entry=" + e);
        }

        return new CommandResponse("OK", null, "AppendEntries aceito");
    }

}
