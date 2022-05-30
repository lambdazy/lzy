package ru.yandex.cloud.ml.platform.lzy.commands;

import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.commands.builtin.Cat;
import ru.yandex.cloud.ml.platform.lzy.commands.builtin.Channel;
import ru.yandex.cloud.ml.platform.lzy.commands.builtin.ChannelStatus;
import ru.yandex.cloud.ml.platform.lzy.commands.builtin.Touch;

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
