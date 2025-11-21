package distribuida.gateway;

import com.sun.net.httpserver.*;
import com.google.gson.Gson;

import distribuida.common.*;
import distribuida.gateway.ServiceRegistry.NodeInfo;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * GatewayMain
 * -----------
 * Este gateway recebe todas as comunicações:
 *
 * - HTTP do cliente (/write, /read)
 * - HTTP dos Nodes (RPCs RAFT)
 * - UDP dos Nodes (REGISTER/HEARTBEAT)
 * - TCP para enviar comandos para os Nodes
 *
 * Toda comunicação Node → Node deve ser roteada aqui.
 */
public class GatewayMain {

    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {

        // Lê arquivo de configuração (porta padrão)
        Properties props = new Properties();
        try (InputStream in = GatewayMain.class.getClassLoader()
                .getResourceAsStream("gateway-config.properties")) {
            if (in != null) props.load(in);
        }

        String host = props.getProperty("gateway.host", "localhost");
        int httpPort = Integer.parseInt(props.getProperty("gateway.http.port", "8080"));
        int tcpPort = Integer.parseInt(props.getProperty("gateway.tcp.port", "8081"));
        int udpPort = Integer.parseInt(props.getProperty("gateway.udp.port", "8082"));
        int registrationPort = Integer.parseInt(props.getProperty("gateway.registration.port", "8000"));

        System.out.println("Gateway starting on host=" + host +
                " http=" + httpPort +
                " tcp=" + tcpPort +
                " udp=" + udpPort +
                " registration=" + registrationPort);

        // Inicia servidores
        startHttpServer(host, httpPort);
        startTcpServer(tcpPort);
        startRegistrationServer(registrationPort);

        System.out.println("Gateway ON: http=" + httpPort +
                " tcp=" + tcpPort +
                " udp=" + udpPort +
                " reg=" + registrationPort);
    }



    // ============================================================
    // 1) HTTP SERVER - para /write, /read, /raft/requestVote e /raft/appendEntries
    // ============================================================

    private static void startHttpServer(String host, int port) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(host, port);
        HttpServer server = HttpServer.create(addr, 0);

        // Saude
        server.createContext("/health", new TextHandler("OK - Gateway HTTP"));

        // Operações normais de key/value
        server.createContext("/write", new WriteHandler());
        server.createContext("/read", new ReadHandler());

        // RPCs RAFT via HTTP
        server.createContext("/raft/requestVote", new RequestVoteHandler());
        server.createContext("/raft/appendEntries", new AppendEntriesHandler());

        server.setExecutor(null);
        server.start();
        System.out.println("[http] Servidor HTTP ouvindo na porta " + port);
    }

    // Handler simples de saúde
    static class TextHandler implements HttpHandler {
        private final String responseText;

        TextHandler(String responseText) {
            this.responseText = responseText;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] bytes = responseText.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }


    // ---------------------- /write ---------------------------
    static class WriteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendPlain(exchange, 405, "Use POST");
                return;
            }

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("[http/write] " + body);

            CommandRequest req = gson.fromJson(body, CommandRequest.class);

            // Busca líder ativo
            var reg = ServiceRegistry.getInstance();
            var leaderOpt = reg.getActiveLeader();

            if (leaderOpt.isEmpty()) {
                sendPlain(exchange, 503, "Nenhum líder ativo");
                return;
            }

            NodeInfo leader = leaderOpt.get();

            CommandResponse resp = sendTcpCommand(leader.ip, leader.port, req);
            sendJson(exchange, 200, gson.toJson(resp));
        }
    }

    // ---------------------- /read ---------------------------
    static class ReadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendPlain(exchange, 405, "Use POST");
                return;
            }

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("[http/read] " + body);

            CommandRequest req = gson.fromJson(body, CommandRequest.class);

            // Pega qualquer nó ativo
            var reg = ServiceRegistry.getInstance();
            var nodeOpt = reg.getAnyActiveReplica();

            if (nodeOpt.isEmpty()) {
                sendPlain(exchange, 503, "Nenhum nó ativo");
                return;
            }

            NodeInfo node = nodeOpt.get();
            CommandResponse resp = sendTcpCommand(node.ip, node.port, req);

            sendJson(exchange, 200, gson.toJson(resp));
        }
    }



    // ============================================================
    // 2) /raft/requestVote - RPC de eleição RAFT via HTTP
    // ============================================================

    static class RequestVoteHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("[raft/requestVote] recebido: " + body);

            RequestVoteRequest req = gson.fromJson(body, RequestVoteRequest.class);

            // Identifica o nó alvo
            var reg = ServiceRegistry.getInstance();
            var target = reg.getActiveNodes().stream()
                    .filter(n -> n.nodeId.equals(req.targetNodeId))
                    .findFirst();

            if (target.isEmpty()) {
                sendPlain(exchange, 404, "Nó alvo não encontrado");
                return;
            }

            NodeInfo dest = target.get();

            // envia via TCP para o nó
            RequestVoteResponse tcpResp = sendTcpRequestVote(dest.ip, dest.port, req);

            sendJson(exchange, 200, gson.toJson(tcpResp));
        }
    }


    // ============================================================
    // 3) /raft/appendEntries - replicação de log via HTTP
    // ============================================================

    static class AppendEntriesHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            System.out.println("[raft/appendEntries] recebido: " + body);

            AppendEntriesRequest req = gson.fromJson(body, AppendEntriesRequest.class);

            var reg = ServiceRegistry.getInstance();
            var target = reg.getActiveNodes().stream()
                    .filter(n -> n.nodeId.equals(req.targetNodeId))
                    .findFirst();

            if (target.isEmpty()) {
                sendPlain(exchange, 404, "Nó alvo não encontrado");
                return;
            }

            NodeInfo dest = target.get();

            AppendEntriesResponse tcpResp = sendTcpAppendEntries(dest.ip, dest.port, req);

            sendJson(exchange, 200, gson.toJson(tcpResp));
        }
    }


    // ============================================================
    // 4) TCP server interno — opcional / echo / RAFT futuro
    // ============================================================

    private static void startTcpServer(int port) {
        Thread t = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(port)) {
                System.out.println("[tcp] Servidor interno na porta " + port);
                while (true) {
                    Socket client = server.accept();
                    new Thread(() -> handleTcpClient(client)).start();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t.setDaemon(true);
        t.start();
    }

    private static void handleTcpClient(Socket c) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(c.getOutputStream()))) {

            String line = in.readLine();
            System.out.println("[tcp/internal] recv: " + line);

            out.write("[echo]" + line);
            out.newLine();
            out.flush();

        } catch (Exception ignore) {}
    }



    // ============================================================
    // 5) UDP 8000 — REGISTER e HEARTBEAT
    // ============================================================

    private static void startRegistrationServer(int port) {
        Thread t = new Thread(() -> {

            try (DatagramSocket socket = new DatagramSocket(port)) {

                System.out.println("[reg] ouvindo em UDP " + port);

                byte[] buf = new byte[2048];
                ServiceRegistry reg = ServiceRegistry.getInstance();

                while (true) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);

                    String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    System.out.println("[reg] recebido: " + msg);

                    String[] p = msg.split("\\s+");

                    if (p[0].equalsIgnoreCase("REGISTER")) {
                        String nodeId = p[1];
                        String ip     = p[2];
                        int nport     = Integer.parseInt(p[3]);
                        String role   = p[4];

                        reg.registerNode(nodeId, ip, nport, role);

                        // envia peers atualizados
                        String peersJson = reg.buildPeersJson();
                        byte[] respBytes = peersJson.getBytes(StandardCharsets.UTF_8);
                        socket.send(new DatagramPacket(
                                respBytes, respBytes.length,
                                packet.getAddress(), packet.getPort()
                        ));

                    } else if (p[0].equalsIgnoreCase("HEARTBEAT")) {

                        reg.updateHeartbeat(p[1]);

                        // resposta simples
                        String resp = "OK";
                        byte[] r = resp.getBytes(StandardCharsets.UTF_8);
                        socket.send(new DatagramPacket(
                                r, r.length,
                                packet.getAddress(), packet.getPort()
                        ));
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        });

        t.setDaemon(true);
        t.start();
    }



    // ============================================================
    // FUNÇÕES DE ENVIO TCP PARA OS NODES
    // ============================================================

    private static CommandResponse sendTcpCommand(String ip, int port, CommandRequest req) {

        String json = gson.toJson(req);

        try (Socket socket = new Socket(ip, port);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.write(json);
            out.newLine();
            out.flush();

            String resp = in.readLine();
            return gson.fromJson(resp, CommandResponse.class);

        } catch (Exception e) {
            return new CommandResponse("ERROR", null, e.getMessage());
        }
    }

    private static RequestVoteResponse sendTcpRequestVote(String ip, int port, RequestVoteRequest req) {

        String json = gson.toJson(req);

        try (Socket socket = new Socket(ip, port);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.write(json);
            out.newLine();
            out.flush();

            String resp = in.readLine();
            return gson.fromJson(resp, RequestVoteResponse.class);

        } catch (Exception e) {
            return new RequestVoteResponse(req.term, false);
        }
    }

    private static AppendEntriesResponse sendTcpAppendEntries(String ip, int port, AppendEntriesRequest req) {

        String json = gson.toJson(req);

        try (Socket socket = new Socket(ip, port);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.write(json);
            out.newLine();
            out.flush();

            String resp = in.readLine();
            return gson.fromJson(resp, AppendEntriesResponse.class);

        } catch (Exception e) {
            return new AppendEntriesResponse(req.term, false);
        }
    }


    // ============================================================
    // AUXILIARES HTTP
    // ============================================================

    private static void sendPlain(HttpExchange ex, int code, String msg) throws IOException {
        byte[] b = msg.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    private static void sendJson(HttpExchange ex, int code, String json) throws IOException {
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }
}
