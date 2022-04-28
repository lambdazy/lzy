package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;

public enum LzyCommands {
    publish(new Publish()),
    terminal(new Terminal()),
    update(new Update()),
    run(new Run()),
    ts(new TasksStatus()),
    kill(new Kill()),
    status(new TerminalStatus()),
    whiteboard(new Whiteboard()),
    storage(new Storage()),
    snapshot(new Snapshot()),
    sessions(new Sessions()),
    start(new Start()),
    cache(new Cache());

    private final LzyCommand command;

    LzyCommands(LzyCommand command) {
        this.command = command;
    }

    public int execute(CommandLine line) throws Exception {
        return command.execute(line);
    }
}
