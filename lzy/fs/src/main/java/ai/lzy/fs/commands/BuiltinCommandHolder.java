package ai.lzy.fs.commands;

import ai.lzy.fs.commands.builtin.Cat;
import org.apache.commons.cli.CommandLine;

public enum BuiltinCommandHolder implements CommandHolder {
    cat(new Cat());

    private final LzyCommand command;

    BuiltinCommandHolder(LzyCommand command) {
        this.command = command;
    }

    @Override
    public LzyCommand command() {
        return command;
    }

    @Override
    public int execute(CommandLine line) throws Exception {
        return command.execute(line);
    }
}
