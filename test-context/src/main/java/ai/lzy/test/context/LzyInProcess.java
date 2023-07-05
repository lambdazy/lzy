package ai.lzy.test.context;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LzyInProcess {
    private final Path pathToAppJar;
    private Process process;

    public LzyInProcess(Path pathToAppJar) {
        if (!pathToAppJar.isAbsolute()) {
            throw new IllegalStateException("Path to jar with application must be an absolute");
        }
        this.pathToAppJar = pathToAppJar;
    }

    public void before(List<String> args) throws Throwable {
        var commands = new ArrayList<String>() {
            {
                add("java -jar");
                add(pathToAppJar.toString());
                addAll(args);
            }
        };
        process = new ProcessBuilder(commands.toArray(new String[0])).inheritIO().start();
    }

    public void after() {
        process.destroy();
        try {
            process.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            process.destroyForcibly();
        }
    }
}
