package ai.lzy.fs.commands;

import org.apache.commons.cli.CommandLine;

public interface CommandHolder {
    LzyCommand command();
    int execute(CommandLine line) throws Exception;
}
