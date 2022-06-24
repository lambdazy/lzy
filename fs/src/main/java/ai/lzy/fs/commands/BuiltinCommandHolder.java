package ai.lzy.fs.commands;

import ai.lzy.fs.commands.builtin.Cat;
import ai.lzy.fs.commands.builtin.Channel;
import ai.lzy.fs.commands.builtin.ChannelStatus;
import ai.lzy.fs.commands.builtin.Touch;
import org.apache.commons.cli.CommandLine;

public enum BuiltinCommandHolder implements CommandHolder {
    cat(new Cat()),
    channel(new Channel()),
    cs(new ChannelStatus()),
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
