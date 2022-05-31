package ru.yandex.cloud.ml.platform.lzy.commands;

import org.apache.commons.cli.CommandLine;

public interface CommandHolder {
    LzyCommand command();
    int execute(CommandLine line) throws Exception;
}
