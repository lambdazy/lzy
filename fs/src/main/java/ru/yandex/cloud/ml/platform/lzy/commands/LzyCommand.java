package ru.yandex.cloud.ml.platform.lzy.commands;

import org.apache.commons.cli.CommandLine;

public interface LzyCommand {
    int execute(CommandLine command) throws Exception;
}
