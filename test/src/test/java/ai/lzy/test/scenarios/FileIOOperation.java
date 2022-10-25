package ai.lzy.test.scenarios;

import ai.lzy.model.deprecated.AtomicZygote;
import ai.lzy.model.graph.AuxEnv;
import ai.lzy.model.graph.BaseEnv;
import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.TextLinesInSlot;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.v1.deprecated.LzyZygote;

import java.util.ArrayList;
import java.util.List;

class FileIOOperation implements AtomicZygote {

    private final String operationName;
    private final List<TextLinesInSlot> inputs;
    private final List<TextLinesOutSlot> outputs;
    private final String command;
    private final Env env;

    FileIOOperation(String operationName, List<String> inputFiles, List<String> outputFiles, String command) {
        this(operationName, inputFiles, outputFiles, command, null);
    }

    FileIOOperation(String operationName, List<String> inputFiles, List<String> outputFiles, String command,
        String baseEnv)
    {
        this.operationName = operationName;
        inputs = new ArrayList<>();
        outputs = new ArrayList<>();
        this.command = command;
        for (String path : inputFiles) {
            inputs.add(new TextLinesInSlot(path));
        }
        for (String path : outputFiles) {
            outputs.add(new TextLinesOutSlot(path));
        }
        env = baseEnv == null ? null : new Env() {
            @Override
            public BaseEnv baseEnv() {
                return () -> baseEnv;
            }

            @Override
            public AuxEnv auxEnv() {
                return null;
            }
        };
    }

    public String getCommand() {
        return command;
    }

    @Override
    public String name() {
        return operationName;
    }

    @Override
    public Slot[] input() {
        return inputs.toArray(new TextLinesInSlot[0]);
    }

    @Override
    public Slot[] output() {
        return outputs.toArray(new TextLinesOutSlot[0]);
    }

    @Override
    public void run() {
    }

    @Override
    public Env env() {
        return env;
    }

    @Override
    public String description() {
        return "Some description";
    }

    @Override
    public Provisioning provisioning() {
        return new Provisioning.Any();
    }

    @Override
    public LzyZygote.Zygote zygote() {
        return null;
    }

    @Override
    public String fuze() {
        return getCommand();
    }

}
