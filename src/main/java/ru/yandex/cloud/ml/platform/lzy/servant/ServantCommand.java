package ru.yandex.cloud.ml.platform.lzy.servant;

import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.Publish;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.Run;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.Terminal;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.Update;

public interface ServantCommand {
    int execute(CommandLine command) throws Exception;

    enum Commands {
        publish(new Publish()),
        terminal(new Terminal()),
        update(new Update()),
        run(new Run());

        private final ServantCommand command;
        Commands(ServantCommand command) {
            this.command = command;
        }

        public int execute(CommandLine line) throws Exception {
            return command.execute(line);
        }
    }
}
