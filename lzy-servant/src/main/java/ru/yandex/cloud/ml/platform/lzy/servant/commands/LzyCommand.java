package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;

public interface LzyCommand {
    int execute(CommandLine command) throws Exception;

    enum Commands {
        publish(new Publish()),
        terminal(new Terminal()),
        update(new Update()),
        run(new Run()),
        channel(new Channel()),
        cs(new ChannelsStatus()),
        ts(new TasksStatus()),
        kill(new TasksStatus()),
        touch(new Touch()),
        status(new TerminalStatus()),
        whiteboard(new Whiteboard());

        private final LzyCommand command;
        Commands(LzyCommand command) {
            this.command = command;
        }

        public int execute(CommandLine line) throws Exception {
            return command.execute(line);
        }
    }
}
