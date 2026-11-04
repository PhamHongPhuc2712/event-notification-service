package com.eventnotification.chaos;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** One of the project's packaged services, running as a separate JVM that can be killed. */
final class ServiceProcess {

    private static final Path PROJECT_ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final Path LOG_DIR = Path.of("target", "chaos-logs");
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private final String name;
    private final int port;
    private final Process process;

    private ServiceProcess(String name, int port, Process process) {
        this.name = name;
        this.port = port;
        this.process = process;
    }

    /**
     * Starts {@code <module>/target/<module>-0.0.1-SNAPSHOT.jar} with the given Spring arguments.
     * The working directory is this module's target folder, so no local infra/.env is picked up.
     */
    static ServiceProcess start(String module, String instanceName, List<String> springArgs) throws IOException {
        int port = freePort();
        Path jar = PROJECT_ROOT.resolve(module).resolve("target").resolve(module + "-0.0.1-SNAPSHOT.jar");
        if (!Files.exists(jar)) {
            throw new IllegalStateException("Missing " + jar + "; run ./mvnw -Pchaos verify from the project root");
        }
        Files.createDirectories(LOG_DIR);
        List<String> command = new ArrayList<>(List.of(javaExecutable(), "-Xmx384m", "-jar", jar.toString(),
                "--server.port=" + port, "--management.health.mail.enabled=false"));
        command.addAll(springArgs);
        Process process = new ProcessBuilder(command)
                .directory(Path.of("target").toFile())
                .redirectErrorStream(true)
                .redirectOutput(LOG_DIR.resolve(instanceName + ".log").toFile())
                .start();
        return new ServiceProcess(instanceName, port, process);
    }

    String name() {
        return name;
    }

    URI baseUrl() {
        return URI.create("http://localhost:" + port);
    }

    /** Waits until the health endpoint reports UP. */
    ServiceProcess awaitHealthy(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        HttpRequest health = HttpRequest.newBuilder(baseUrl().resolve("/actuator/health"))
                .timeout(Duration.ofSeconds(2)).build();
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) {
                throw new IllegalStateException(name + " exited with " + process.exitValue()
                        + "; see target/chaos-logs/" + name + ".log");
            }
            try {
                HttpResponse<String> response = HTTP.send(health, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200 && response.body().contains("\"UP\"")) {
                    return this;
                }
            } catch (IOException e) {
                // still starting
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException(name + " was not healthy after " + timeout);
    }

    /** Like {@code kill -9}: no shutdown hooks, no offset commits, no cleanup. */
    void kill() throws InterruptedException {
        process.destroyForcibly();
        process.waitFor();
    }

    private static String javaExecutable() {
        return ProcessHandle.current().info().command().orElse("java");
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
