package distribuida.node;

import com.google.gson.Gson;
import distribuida.common.CommandRequest;
import distribuida.common.CommandResponse;
import distribuida.common.LogEntry;
import distribuida.common.NodeRole;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Processo de nó (Leader ou Follower)
 *
 * Ao iniciar:
 *   1) Envia REGISTER para o Gateway (UDP 8000)
 *   2) Inicia thread de HEARTBEAT (UDP simples para o Gateway)
 *   3) Inicia lógica simplificada de eleição (RAFT)
 *   4) Abre servidor TCP na porta local (--port=xxxx)
 */
public class NodeMain {

    // ----------------- CONFIG BÁSICA DO NÓ -----------------
    private static String nodeId;
    private static String role;           // LEADER / FOLLOWER (string recebida por argumento)
    private static String gatewayHost;
    private static int gatewayRegPort;    // 8000
    private static int localPort;         // porta do servidor do nó

    private static final Gson gson = new Gson();

    // ----------------- ESTADO RAFT -----------------
    // papel atual do nó no protocolo
    private static NodeRole nodeRole = NodeRole.FOLLOWER;

    private static int currentTerm = 0;
    private static String votedFor = null;

    // log de comandos (índice, termo, key, value)
    private static final List<LogEntry> logEntries = new ArrayList<>();

    private static int commitIndex = 0;
    private static int lastApplied = 0;

    // timeout de eleição
    private static long nextElectionTime;
    private static final Random rand = new Random();

    // Armazenamento key/value local do nó
    private static final Map<String, String> store = new ConcurrentHashMap<>();

    // -------------------------------------------------------
    // MAIN
    // -------------------------------------------------------
    public static void main(String[] args) throws Exception {

        System.out.println("ARGS RECEIVED:");
        for (String a : args) {
            System.out.println(" > " + a);
        }

        // ----------------------------
        // PARSE DOS ARGUMENTOS
        // ----------------------------
        for (String arg : args) {

            if (arg.startsWith("--nodeId=")) {
                nodeId = arg.substring("--nodeId=".length());
            }
            else if (arg.startsWith("--role=")) {
                role = arg.substring("--role=".length());
            }
            else if (arg.startsWith("--gatewayHost=")) {
                gatewayHost = arg.substring("--gatewayHost=".length());
            }
            else if (arg.startsWith("--gatewayRegPort=")) {
                gatewayRegPort = Integer.parseInt(
                        arg.substring("--gatewayRegPort=".length())
                );
            }
            else if (arg.startsWith("--port=")) {
                localPort = Integer.parseInt(
                        arg.substring("--port=".length())
                );
            }
        }

        // ----------------------------
        // VALIDAÇÃO DOS ARGUMENTOS
        // ----------------------------
        if (nodeId == null) System.out.println("nodeId faltando!");
        if (role == null) System.out.println("role faltando!");
        if (gatewayHost == null) System.out.println("gatewayHost faltando!");
        if (gatewayRegPort == 0) System.out.println("gatewayRegPort faltando!");
        if (localPort == 0) System.out.println("port faltando!");

        if (nodeId == null || role == null || gatewayHost == null ||
                gatewayRegPort == 0 || localPort == 0) {
            System.out.println("Uso correto:");
            System.out.println("java NodeMain --nodeId=A1 --role=LEADER --port=5001 --gatewayHost=localhost --gatewayRegPort=8000");
            return;
        }

        // ajustar estado RAFT inicial com base no argumento --role
        if ("LEADER".equalsIgnoreCase(role)) {
            nodeRole = NodeRole.LEADER;
        } else {
            nodeRole = NodeRole.FOLLOWER;
        }

        System.out.println("[node] Starting nodeId=" + nodeId +
                " role=" + role +
                " localPort=" + localPort +
                " gateway=" + gatewayHost + ":" + gatewayRegPort);

        // 1. Registrar no gateway
        sendRegister();

        // 2. Thread de Heartbeat (UDP simples para Gateway)
        startHeartbeatThread();

        // 3. Lógica de eleição RAFT (simplificada, sem ServiceRegistry)
        resetElectionTimeout();
        startElectionThread();

        // 4. Servidor TCP local
        startLocalTcpServer();

        System.out.println("[node] Node ON");
    }

    // -----------------------------------------------
    // 1) Envia REGISTER para o Gateway
    // -----------------------------------------------
    private static void sendRegister() {
        try (DatagramSocket socket = new DatagramSocket()) {
            String ip = InetAddress.getLocalHost().getHostAddress();
            String msg = "REGISTER " + nodeId + " " + ip + " " + localPort + " " + role;

            byte[] data = msg.getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(
                    data, data.length,
                    InetAddress.getByName(gatewayHost),
                    gatewayRegPort
            );

            socket.send(packet);
            System.out.println("[node] REGISTER sent → " + msg);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // -----------------------------------------------
    // 2) Thread de HEARTBEAT para o Gateway
    // -----------------------------------------------
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
                    // envia heartbeat a cada 2s
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

    // -------------------------------------------------------
    // 3) ELEIÇÃO RAFT (SEM ServiceRegistry, simplificada)
    // -------------------------------------------------------
    private static void resetElectionTimeout() {
        long timeout = 150 + rand.nextInt(150); // entre 150 e 300ms
        nextElectionTime = System.currentTimeMillis() + timeout;
    }

    private static void startElectionThread() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(50);
                } catch (Exception ignored) {}

                long now = System.currentTimeMillis();

                // se não for líder e timeout expirar → vira candidato
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

        // reset para próxima checagem
        resetElectionTimeout();

        // *** SIMPLIFICADO ***
        // sem ServiceRegistry: vamos assumir apenas 1 nó
        int activeNodes = 1;
        int quorum = 1;

        if (activeNodes >= quorum) {
            nodeRole = NodeRole.LEADER;
            System.out.println("[RAFT] " + nodeId + " eleito LEADER em " + currentTerm);

            // ao virar leader, enviamos heartbeats RAFT (AppendEntries vazio)
            startLeaderHeartbeatThread();
        }
    }

    private static void startLeaderHeartbeatThread() {
        Thread t = new Thread(() -> {
            while (nodeRole == NodeRole.LEADER) {
                try {
                    sendAppendEntriesHeartbeat();
                    Thread.sleep(120); // RAFT ~120ms
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }

    private static void sendAppendEntriesHeartbeat() {
        // HEARTBEAT RAFT = AppendEntries vazio (aqui só logamos)
        System.out.println("[RAFT] Leader " + nodeId + " enviando AppendEntries vazio (heartbeat RAFT)...");
    }

    // -----------------------------------------------
    // 4) Servidor TCP local (para receber comandos)
    // -----------------------------------------------
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
                    System.out.println("[node] Conexão recebida de " + client.getRemoteSocketAddress());
                    new Thread(() -> handleClient(client)).start();
                }

            } catch (Exception e) {
                System.err.println("[ERRO] Não foi possível iniciar o servidor TCP:");
                e.printStackTrace();
            }
        });

        t.setDaemon(true);
        t.start();

        System.out.println("[node] Thread TCP iniciada (startLocalTcpServer)");
    }

    // -----------------------------------------------
    // Tratamento de cliente TCP (JSON CommandRequest)
    // -----------------------------------------------
    private static void handleClient(Socket client) {
        String remote = client.getRemoteSocketAddress().toString();
        System.out.println("[node] conexão TCP de " + remote);

        try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("[node] recebido de " + remote + ": " + line);

                CommandResponse resp;
                try {
                    CommandRequest req = gson.fromJson(line, CommandRequest.class);

                    if (req == null || req.type == null) {
                        resp = new CommandResponse("ERROR", null, "Requisição inválida");
                    } else {
                        String type = req.type.toUpperCase();

                        switch (type) {
                            case "WRITE":
                            case "SET":
                                if (req.key == null) {
                                    resp = new CommandResponse("ERROR", null, "Chave ausente");
                                } else {
                                    int newIndex = logEntries.size() + 1;

                                    LogEntry entry = new LogEntry(
                                            newIndex,
                                            currentTerm,
                                            req.key,
                                            req.value
                                    );
                                    logEntries.add(entry);

                                    System.out.println("[RAFT] Leader recebeu WRITE → nova logEntry: " + entry);

                                    // TODO: replicar para followers (futuro)
                                    store.put(req.key, req.value);

                                    System.out.println("[node] gravado key=" + req.key + " value=" + req.value);
                                    resp = new CommandResponse("OK", null, "Valor gravado");
                                }
                                break;

                            case "READ":
                            case "GET":
                                if (req.key == null) {
                                    resp = new CommandResponse("ERROR", null, "Chave ausente");
                                } else {
                                    String value = store.get(req.key);
                                    if (value == null) {
                                        resp = new CommandResponse("ERROR", null, "Chave não encontrada");
                                    } else {
                                        resp = new CommandResponse("OK", value, "Valor lido com sucesso");
                                    }
                                }
                                break;

                            default:
                                resp = new CommandResponse("ERROR", null, "Tipo de operação desconhecido: " + req.type);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    resp = new CommandResponse("ERROR", null, "Falha ao processar JSON: " + e.getMessage());
                }

                String jsonResp = gson.toJson(resp);
                out.println(jsonResp);
                System.out.println("[node] resposta enviada para " + remote + ": " + jsonResp);
            }

        } catch (IOException e) {
            System.err.println("[node] erro com cliente " + remote + ": " + e.getMessage());
        }
    }
}
