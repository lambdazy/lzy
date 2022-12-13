package ai.lzy.fs.commands;

import ai.lzy.fs.commands.builtin.Cat;
import ai.lzy.fs.commands.builtin.Touch;
import org.apache.commons.cli.CommandLine;

public enum BuiltinCommandHolder implements CommandHolder {
    cat(new Cat()),
    touch(new Touch());

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
