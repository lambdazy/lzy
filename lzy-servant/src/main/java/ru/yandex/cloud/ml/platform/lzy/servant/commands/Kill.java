package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.NotImplementedException;

public class Kill implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        throw new NotImplementedException("Kill is not implemented yet");
    }
}
