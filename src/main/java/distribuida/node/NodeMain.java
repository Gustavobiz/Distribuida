package distribuida.node;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Processo de nó (Leader ou Follower)
 *
 * Ao iniciar:
 *   1) Envia REGISTER para o Gateway (UDP 8000)
 *   2) Inicia thread de HEARTBEAT
 *   3) Abre servidor TCP na porta local escolhida (--port=xxxx)
 */
public class NodeMain {

    private static String nodeId;
    private static String role;           // LEADER / FOLLOWER
    private static String gatewayHost;
    private static int gatewayRegPort;    // 8000
    private static int localPort;         // porta do servidor do nó

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

        System.out.println("[node] Starting nodeId=" + nodeId +
                " role=" + role +
                " localPort=" + localPort +
                " gateway=" + gatewayHost + ":" + gatewayRegPort);

        // 1. Registrar no gateway
        sendRegister();

        // 2. Thread de Heartbeat
        startHeartbeatThread();

        // 3. Servidor TCP local
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
    // 2) Thread de HEARTBEAT
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

    // -----------------------------------------------
    // 3) Servidor TCP local (para receber comandos)
    // -----------------------------------------------
private static void startLocalTcpServer() {
    System.out.println("[node] Iniciando thread do servidor TCP...");

    Thread t = new Thread(() -> {
        System.out.println("[node] Thread do servidor TCP iniciada.");

        try {
            System.out.println("[node] Tentando abrir a porta TCP " + localPort + "...");

            // ATENÇÃO: NÃO USE try-with-resources AQUI
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

    t.start();

    System.out.println("[node] Thread TCP iniciada (startLocalTcpServer)");
}


    private static void handleClient(Socket client) {
        String remote = client.getRemoteSocketAddress().toString();
        System.out.println("[node] connection from " + remote);

        try (BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {

            String line;
            while ((line = in.readLine()) != null) {

                System.out.println("[node] recv: " + line + " from " + remote);

                // por enquanto só ecoa
                out.println("[node/" + nodeId + "] echo: " + line);
            }

        } catch (IOException e) {
            System.err.println("[node] client error: " + e.getMessage());
        }
    }
}
