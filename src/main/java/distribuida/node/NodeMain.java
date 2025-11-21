package distribuida.node;

import com.google.gson.Gson;
import distribuida.common.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * NodeMain
 * --------
 * Nó de dados (A1, A2, B1...) com:
 *
 * - Registro e heartbeat no Gateway via UDP
 * - Servidor TCP para receber comandos e RPCs RAFT
 * - RAFT simplificado (eleição, requestVote, appendEntries)
 * - Key/Value Store em memória
 *
 * Toda comunicação nó ↔ nó passa pelo Gateway:
 * Node -> HTTP -> Gateway -> TCP -> Node
 */
public class NodeMain {

    // -------------------------
    // Parâmetros de processo
    // -------------------------
    private static String nodeId;
    private static String roleArg;        // "LEADER" ou "FOLLOWER"
    private static String gatewayHost;
    private static int gatewayRegPort;    // UDP (ex: 8000)
    private static int localPort;         // TCP do nó

    // Porta HTTP do Gateway (pode ajustar se mudar o config)
    private static final int GATEWAY_HTTP_PORT = 8080;

    // -------------------------
    // Estado RAFT
    // -------------------------
    private static NodeRole nodeRole = NodeRole.FOLLOWER;

    private static int currentTerm = 0;
    private static String votedFor = null;

    private static final List<LogEntry> logEntries = new ArrayList<>();
    private static int commitIndex = 0;
    private static int lastApplied = 0;

    private static long electionDeadlineMs = 0L;
    private static final Random rand = new Random();

    // Lista de peers recebidos do Gateway (A1, A2, B1...)
    private static final List<PeerInfo> peers = new ArrayList<>();

    // -------------------------
    // State Machine: Key/Value
    // -------------------------
    private static final Map<String, String> kvStore = new ConcurrentHashMap<>();

    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {

        System.out.println("ARGS RECEIVED:");
        for (String a : args) System.out.println(" > " + a);

        // 1) Parse simples dos argumentos
        for (String arg : args) {
            if (arg.startsWith("--nodeId=")) {
                nodeId = arg.substring("--nodeId=".length());
            } else if (arg.startsWith("--role=")) {
                roleArg = arg.substring("--role=".length());
            } else if (arg.startsWith("--gatewayHost=")) {
                gatewayHost = arg.substring("--gatewayHost=".length());
            } else if (arg.startsWith("--gatewayRegPort=")) {
                gatewayRegPort = Integer.parseInt(arg.substring("--gatewayRegPort=".length()));
            } else if (arg.startsWith("--port=")) {
                localPort = Integer.parseInt(arg.substring("--port=".length()));
            }
        }

        if (nodeId == null || roleArg == null || gatewayHost == null || gatewayRegPort == 0 || localPort == 0) {
            System.out.println("Uso correto:");
            System.out.println("java NodeMain --nodeId=A1 --role=LEADER --port=6000 --gatewayHost=localhost --gatewayRegPort=8000");
            return;
        }

        nodeRole = roleArg.equalsIgnoreCase("LEADER") ? NodeRole.LEADER : NodeRole.FOLLOWER;

        System.out.println("[node] Iniciando nodeId=" + nodeId +
                " role=" + nodeRole +
                " localPort=" + localPort +
                " gateway=" + gatewayHost + ":" + gatewayRegPort);

        // 2) Registrar no Gateway via UDP (recebe peers em JSON)
        sendRegister();

        // 3) Inicia heartbeat para o Gateway (UDP HEARTBEAT)
        startHeartbeatThread();

        // 4) Inicia servidor TCP para comandos/RAFT
        startLocalTcpServer();

        // 5) Inicia RAFT (eleição + heartbeat de líder)
        resetElectionTimeout();
        startElectionTimerThread();

        if (nodeRole == NodeRole.LEADER) {
            startLeaderHeartbeatThread();
        }

        System.out.println("[node] Node ON");
    }

    // ==========================================================
    // REGISTRO & HEARTBEAT
    // ==========================================================

    /**
     * Envia REGISTER via UDP para o Gateway, e recebe lista de peers em JSON.
     */
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

            // Lê resposta com peers
            byte[] buf = new byte[4096];
            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            socket.receive(resp);

            String json = new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8);
            System.out.println("[node] REGISTER response → " + json);

            // Parse do JSON de peers: { "peers": [ {nodeId,ip,port}, ... ] }
            PeersResponse peersResp = gson.fromJson(json, PeersResponse.class);
            if (peersResp != null && peersResp.peers != null) {
                peers.clear();
                peers.addAll(peersResp.peers);
                System.out.println("[node] Peers atualizados: " + peers.size());
            }

        } catch (Exception e) {
            System.err.println("[node] Erro no REGISTER: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Thread que envia HEARTBEAT periódicos (UDP) para o Gateway.
     */
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

    // ==========================================================
    // RAFT: ELEIÇÃO + HEARTBEAT
    // ==========================================================

    private static synchronized void resetElectionTimeout() {
        // timeout aleatório entre 150ms e 300ms
        int timeout = 150 + rand.nextInt(150);
        electionDeadlineMs = System.currentTimeMillis() + timeout;
    }

    /**
     * Thread que verifica timeout de eleição.
     * Se for follower e não receber AppendEntries/RequestVote a tempo -> vira Candidate.
     */
    private static void startElectionTimerThread() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {}

                synchronized (NodeMain.class) {
                    if (nodeRole == NodeRole.LEADER) {
                        // Líder não faz eleição
                        continue;
                    }
                    long now = System.currentTimeMillis();
                    if (now >= electionDeadlineMs) {
                        startElection();
                    }
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    /**
     * Inicia eleição RAFT: vira Candidate, incrementa term, pede votos via Gateway (/raft/requestVote).
     */
    private static void startElection() {
        synchronized (NodeMain.class) {
            currentTerm++;
            nodeRole = NodeRole.CANDIDATE;
            votedFor = nodeId;
            System.out.println("[RAFT] " + nodeId + " virou CANDIDATE no term " + currentTerm);

            int votes = 1; // voto em si mesmo

            int lastLogIndex = logEntries.size();
            int lastLogTerm = (lastLogIndex == 0) ? 0 : logEntries.get(lastLogIndex - 1).term;

            resetElectionTimeout();

            // Pede votos para todos os peers (exceto ele mesmo)
            for (PeerInfo p : peers) {
                if (p.nodeId.equals(nodeId)) continue;

                RequestVoteRequest req = new RequestVoteRequest();
                req.term = currentTerm;
                req.candidateId = nodeId;
                req.lastLogIndex = lastLogIndex;
                req.lastLogTerm = lastLogTerm;
                req.targetNodeId = p.nodeId;

                RequestVoteResponse resp = sendHttpRequestVote(req);
                if (resp != null && resp.voteGranted && resp.term == currentTerm) {
                    votes++;
                } else if (resp != null && resp.term > currentTerm) {
                    // Encontrou termo maior, volta a ser follower
                    currentTerm = resp.term;
                    nodeRole = NodeRole.FOLLOWER;
                    votedFor = null;
                    resetElectionTimeout();
                    return;
                }
            }

            int clusterSize = Math.max(1, peers.size());
            int majority = (clusterSize / 2) + 1;

            if (votes >= majority) {
                nodeRole = NodeRole.LEADER;
                System.out.println("[RAFT] " + nodeId + " foi ELEITO LEADER no term " + currentTerm +
                        " com " + votes + " votos (majority=" + majority + ")");
                startLeaderHeartbeatThread();
            } else {
                System.out.println("[RAFT] " + nodeId + " NÃO conseguiu maioria (votes=" + votes +
                        ", majority=" + majority + "), voltando a FOLLOWER");
                nodeRole = NodeRole.FOLLOWER;
                votedFor = null;
                resetElectionTimeout();
            }
        }
    }

    /**
     * Thread de heartbeat do líder: envia AppendEntries vazio para todos os peers via Gateway.
     */
    private static void startLeaderHeartbeatThread() {
        Thread t = new Thread(() -> {
            while (true) {
                synchronized (NodeMain.class) {
                    if (nodeRole != NodeRole.LEADER) {
                        break;
                    }
                }

                // Para cada peer, envia AppendEntries (heartbeat)
                int lastLogIndex = logEntries.size();
                int lastLogTerm = (lastLogIndex == 0) ? 0 : logEntries.get(lastLogIndex - 1).term;

                for (PeerInfo p : peers) {
                    if (p.nodeId.equals(nodeId)) continue;

                    AppendEntriesRequest req = new AppendEntriesRequest();
                    req.term = currentTerm;
                    req.leaderId = nodeId;
                    req.prevLogIndex = lastLogIndex;
                    req.prevLogTerm = lastLogTerm;
                    req.leaderCommit = commitIndex;
                    req.targetNodeId = p.nodeId;
                    req.entries = Collections.emptyList(); // heartbeat vazio

                    AppendEntriesResponse resp = sendHttpAppendEntries(req);
                    if (resp != null && !resp.success && resp.term > currentTerm) {
                        synchronized (NodeMain.class) {
                            currentTerm = resp.term;
                            nodeRole = NodeRole.FOLLOWER;
                            votedFor = null;
                            resetElectionTimeout();
                        }
                        break;
                    }
                }

                try {
                    Thread.sleep(150);
                } catch (InterruptedException ignored) {}
            }
        });
        t.setDaemon(true);
        t.start();
    }

    // ==========================================================
    // CLIENTE HTTP RAFT (Node -> Gateway -> Node)
    // ==========================================================

    private static RequestVoteResponse sendHttpRequestVote(RequestVoteRequest req) {
        try {
            URL url = new URL("http://" + gatewayHost + ":" + GATEWAY_HTTP_PORT + "/raft/requestVote");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

            String json = gson.toJson(req);
            try (OutputStream os = con.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            try (InputStream is = con.getInputStream()) {
                String resp = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                return gson.fromJson(resp, RequestVoteResponse.class);
            }
        } catch (Exception e) {
            System.err.println("[RAFT] erro em sendHttpRequestVote: " + e.getMessage());
            return null;
        }
    }

    private static AppendEntriesResponse sendHttpAppendEntries(AppendEntriesRequest req) {
        try {
            URL url = new URL("http://" + gatewayHost + ":" + GATEWAY_HTTP_PORT + "/raft/appendEntries");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

            String json = gson.toJson(req);
            try (OutputStream os = con.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            try (InputStream is = con.getInputStream()) {
                String resp = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                return gson.fromJson(resp, AppendEntriesResponse.class);
            }
        } catch (Exception e) {
            System.err.println("[RAFT] erro em sendHttpAppendEntries: " + e.getMessage());
            return null;
        }
    }

    // ==========================================================
    // SERVIDOR TCP LOCAL (comandos + RAFT RPCs vindos do Gateway)
    // ==========================================================

    private static void startLocalTcpServer() {
        Thread t = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(localPort)) {
                System.out.println("[node] Servidor TCP ATIVO na porta " + localPort);
                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(() -> handleClient(client)).start();
                }
            } catch (IOException e) {
                System.err.println("[node] TCP server error: " + e.getMessage());
            }
        });
        t.setDaemon(true);
        t.start();
        System.out.println("[node] Iniciando thread do servidor TCP...");
    }

    private static void handleClient(Socket client) {
        String remote = client.getRemoteSocketAddress().toString();
        System.out.println("[node] conexão de " + remote);

        try (BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("[node] recebido de " + remote + ": " + line);

                // Decide se é RPC RAFT ou comando normal
                if (line.contains("\"candidateId\"") && line.contains("\"lastLogIndex\"")) {
                    // RequestVote
                    RequestVoteRequest req = gson.fromJson(line, RequestVoteRequest.class);
                    RequestVoteResponse resp = handleRequestVote(req);
                    out.println(gson.toJson(resp));
                } else if (line.contains("\"leaderId\"") && line.contains("\"prevLogIndex\"")) {
                    // AppendEntries
                    AppendEntriesRequest req = gson.fromJson(line, AppendEntriesRequest.class);
                    AppendEntriesResponse resp = handleAppendEntries(req);
                    out.println(gson.toJson(resp));
                } else {
                    // Comando normal WRITE/READ vindo do Gateway (/write, /read)
                    CommandRequest req = gson.fromJson(line, CommandRequest.class);
                    CommandResponse resp = processCommand(req);
                    out.println(gson.toJson(resp));
                }
            }

        } catch (Exception e) {
            System.err.println("[node] erro com cliente " + remote + ": " + e.getMessage());
        }
    }

    // ==========================================================
    // PROCESSAMENTO RAFT: RequestVote / AppendEntries
    // ==========================================================

    private static synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {

        if (req.term < currentTerm) {
            return new RequestVoteResponse(currentTerm, false);
        }

        if (req.term > currentTerm) {
            currentTerm = req.term;
            nodeRole = NodeRole.FOLLOWER;
            votedFor = null;
        }

        boolean canVote = (votedFor == null || votedFor.equals(req.candidateId));
        boolean voteGranted = false;

        if (canVote) {
            votedFor = req.candidateId;
            voteGranted = true;
            resetElectionTimeout();
            System.out.println("[RAFT] " + nodeId + " votou para " + req.candidateId + " no term " + currentTerm);
        }

        return new RequestVoteResponse(currentTerm, voteGranted);
    }

    private static synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {

        if (req.term < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false);
        }

        // Atualiza termo e vira follower, pois encontrou um líder válido
        currentTerm = req.term;
        nodeRole = NodeRole.FOLLOWER;
        resetElectionTimeout();

        // Simplificação: não checa prevLogIndex/prevLogTerm, apenas aplica
        if (req.entries != null && !req.entries.isEmpty()) {
            for (LogEntry e : req.entries) {
                logEntries.add(e);
                // aplica no KV imediatamente
                if (e.key != null) {
                    kvStore.put(e.key, e.value);
                    System.out.println("[RAFT] " + nodeId + " aplicou entry key=" + e.key + " value=" + e.value);
                }
            }
            commitIndex = logEntries.size();
        }

        return new AppendEntriesResponse(currentTerm, true);
    }

    // ==========================================================
    // PROCESSAMENTO DE COMANDOS (WRITE/READ)
    // ==========================================================

    private static CommandResponse processCommand(CommandRequest req) {
        if (req == null || req.type == null) {
            return new CommandResponse("ERROR", null, "Requisição inválida");
        }

        switch (req.type.toUpperCase()) {
            case "WRITE":
            case "SET":
                return handleWrite(req);
            case "READ":
            case "GET":
                return handleRead(req);
            default:
                return new CommandResponse("ERROR", null, "Tipo desconhecido: " + req.type);
        }
    }

    private static CommandResponse handleWrite(CommandRequest req) {
        synchronized (NodeMain.class) {
            if (nodeRole != NodeRole.LEADER) {
                return new CommandResponse("ERROR", null, "Nó " + nodeId + " não é LEADER");
            }

            int newIndex = logEntries.size() + 1;
            LogEntry entry = new LogEntry(newIndex, currentTerm, req.key, req.value);
            logEntries.add(entry);

            // Aplica localmente
            kvStore.put(req.key, req.value);
            commitIndex = newIndex;
            System.out.println("[RAFT] LEADER " + nodeId + " gravou entry=" + entry);

            // Replicação simplificada: manda AppendEntries com 1 entry para cada follower
            for (PeerInfo p : peers) {
                if (p.nodeId.equals(nodeId)) continue;

                AppendEntriesRequest areq = new AppendEntriesRequest();
                areq.term = currentTerm;
                areq.leaderId = nodeId;
                areq.prevLogIndex = newIndex - 1;
                areq.prevLogTerm = (newIndex == 1) ? 0 : logEntries.get(newIndex - 2).term;
                areq.leaderCommit = commitIndex;
                areq.targetNodeId = p.nodeId;
                areq.entries = Collections.singletonList(entry);

                AppendEntriesResponse aresp = sendHttpAppendEntries(areq);
                if (aresp == null || !aresp.success) {
                    System.out.println("[RAFT] Falha ao replicar para " + p.nodeId);
                }
            }

            return new CommandResponse("OK", null, "Valor gravado com sucesso no líder " + nodeId);
        }
    }

    private static CommandResponse handleRead(CommandRequest req) {
        String v = kvStore.get(req.key);
        if (v == null) {
            return new CommandResponse("ERROR", null, "Chave não encontrada");
        }
        return new CommandResponse("OK", v, "Valor lido com sucesso");
    }

    // ==========================================================
    // CLASSES AUXILIARES INTERNAS
    // ==========================================================

    /**
     * Usada apenas para deserializar a response do REGISTER do Gateway.
     */
    static class PeersResponse {
        List<PeerInfo> peers;
    }
}
