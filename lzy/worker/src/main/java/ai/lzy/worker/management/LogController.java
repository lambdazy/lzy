package ai.lzy.worker.management;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;

@Controller(value = "/log", consumes = MediaType.ALL, produces = MediaType.TEXT_PLAIN)
public class LogController {

    @Post("/{name}/{level}")
    public String configureLogger(String name, String level) {
        Configurator.setLevel(LogManager.getLogger(name), Level.getLevel(level));
        var sb = new StringBuilder();
        for (var log : LogManager.getContext(false).getLoggerRegistry().getLoggers()) {
            sb.append(log.getName()).append('\t').append(log.getLevel()).append('\n');
        }
        return sb.toString();
    }

    @Get("/{name}")
    public String showLogger(String name) {
        var log = LogManager.getLogger(name);
        return log.getName() + '\t' + log.getLevel() + '\n';
    }

    @Get
    public String showLoggers() {
        var sb = new StringBuilder();
        for (var log : LogManager.getContext(false).getLoggerRegistry().getLoggers()) {
            sb.append(log.getName()).append('\t').append(log.getLevel()).append('\n');
        }
        return sb.toString();
    }
}
