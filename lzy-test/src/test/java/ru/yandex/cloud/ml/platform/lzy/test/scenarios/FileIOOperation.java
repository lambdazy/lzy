package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;

import java.util.ArrayList;
import java.util.List;

class FileIOOperation implements AtomicZygote {
    private final String operationName;
    private final List<TextLinesInSlot> inputs;
    private final List<TextLinesOutSlot> outputs;
    private final String command;

    FileIOOperation(String operationName, List<String> inputFiles, List<String> outputFiles, String command) {
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
    }

    public String getCommand() {
        return command;
    }

    public String getName() {
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
        return null;
    }

    @Override
    public Provisioning provisioning() {
        return new Provisioning() {};
    }

    @Override
    public String fuze() {
        return getCommand();
    }
}
