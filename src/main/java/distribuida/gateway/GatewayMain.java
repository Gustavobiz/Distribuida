package distribuida.gateway;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class GatewayMain {

    public static void main(String[] args) throws Exception {
        // 1) Carrega configurações
        Properties props = new Properties();
        try (InputStream in = GatewayMain.class.getClassLoader()
                .getResourceAsStream("gateway-config.properties")) {
            if (in == null) {
                throw new IllegalStateException("gateway-config.properties não encontrado em resources");
            }
            props.load(in);
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

        // 2) Inicia cada servidor em uma thread separada
        startHttpServer(host, httpPort);
        startTcpServer(tcpPort);
        startUdpServer(udpPort);
        startRegistrationServer(registrationPort);

        System.out.println("Gateway ON: http=" + httpPort +
                " tcp=" + tcpPort +
                " udp=" + udpPort +
                " reg=" + registrationPort);
    }

    // ------------------ HTTP 8080 ------------------

    private static void startHttpServer(String host, int port) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(host, port);
        HttpServer server = HttpServer.create(addr, 0);

        // Endpoint de saúde / teste
        server.createContext("/health", new TextHandler("OK - Gateway HTTP"));

        // Endpoint de teste de WRITE (depois vamos rotear para Leader)
        server.createContext("/write", new EchoHandler("WRITE"));

        // Endpoint de teste de READ (depois vamos rotear para réplicas)
        server.createContext("/read", new EchoHandler("READ"));

        server.setExecutor(null); // default executor
        server.start();
        System.out.println("[http]" + port);
    }

    // Handler simples para /health
    static class TextHandler implements HttpHandler {
        private final String responseText;

        TextHandler(String responseText) {
            this.responseText = responseText;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] bytes = responseText.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    // Por enquanto só ecoa a requisição, depois vai rotear para Leader/Followers
    static class EchoHandler implements HttpHandler {
        private final String type;

        EchoHandler(String type) {
            this.type = type;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String method = exchange.getRequestMethod();
            String response = "[http/" + type + "] method=" + method + " body=" + body;

            System.out.println(response);

            Headers headers = exchange.getResponseHeaders();
            headers.add("Content-Type", "text/plain; charset=UTF-8");
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    // ------------------ TCP 8081 ------------------

    private static void startTcpServer(int port) {
        Thread t = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[tcp]" + port);
                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(() -> handleTcpClient(client)).start();
                }
            } catch (IOException e) {
                System.err.println("[tcp] ERROR: " + e.getMessage());
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private static void handleTcpClient(Socket client) {
        String remote = client.getRemoteSocketAddress().toString();
        System.out.println("[tcp] connection from " + remote);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8))) {

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("[tcp] recv: " + line + " from " + remote);
                // Por enquanto só ecoa. Depois: rotear WRITE/READ para Leader/Follower.
                out.write("[tcp/echo] " + line);
                out.newLine();
                out.flush();
            }
        } catch (IOException e) {
            System.err.println("[tcp] client error: " + e.getMessage());
        } finally {
            try {
                client.close();
            } catch (IOException ignore) {}
        }
    }

    // ------------------ UDP 8082 ------------------

    private static void startUdpServer(int port) {
        Thread t = new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(port)) {
                System.out.println("[udp] " + port);
                byte[] buf = new byte[2048];
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    String remote = packet.getAddress().getHostAddress() + ":" + packet.getPort();
                    System.out.println("[udp] recv from " + remote + ": " + msg);

                    // Por enquanto só ecoa
                    String response = "[udp/echo] " + msg;
                    byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
                    DatagramPacket resp = new DatagramPacket(
                            respBytes, respBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(resp);
                }
            } catch (IOException e) {
                System.err.println("[udp] ERROR: " + e.getMessage());
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();
    }

    // ------------------ Registro / Heartbeat UDP 8000 ------------------

    private static void startRegistrationServer(int port) {
        Thread t = new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(port)) {
                System.out.println("[reg] Listening for register/heartbeat on " + port);
                byte[] buf = new byte[2048];
                ServiceRegistry registry = ServiceRegistry.getInstance();

                while (true) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    String remote = packet.getAddress().getHostAddress() + ":" + packet.getPort();
                    System.out.println("[reg] recv from " + remote + ": " + msg);

                    // Formatos aceitos (simples por enquanto):
                    // REGISTER nodeId ip port role
                    // HEARTBEAT nodeId
                    String[] parts = msg.trim().split("\\s+");
                    if (parts.length >= 2) {
                        String cmd = parts[0].toUpperCase();
                        if ("REGISTER".equals(cmd) && parts.length >= 5) {
                            String nodeId = parts[1];
                            String ip = parts[2];
                            int nodePort = Integer.parseInt(parts[3]);
                            String role = parts[4];
                            registry.registerNode(nodeId, ip, nodePort, role);
                        } else if ("HEARTBEAT".equals(cmd)) {
                            String nodeId = parts[1];
                            registry.updateHeartbeat(nodeId);
                        } else {
                            System.out.println("[reg] comando inválido: " + msg);
                        }
                    }

                    // opcional: responder algo
                    String response = "OK";
                    byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
                    DatagramPacket resp = new DatagramPacket(
                            respBytes, respBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(resp);
                }
            } catch (IOException e) {
                System.err.println("[reg] ERROR: " + e.getMessage());
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
