package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.util.List;
import java.util.stream.Stream;

public interface SnapshotExecution {
    String name();

    Stream<ExecutionArg> outputArgs();

    Stream<InputExecutionArg> inputArgs();

    String snapshotId();

    class SnapshotExecutionImpl implements SnapshotExecution {

        private final String name;
        private final String snapshot;
        private final List<ExecutionArg> output;
        private final List<InputExecutionArg> input;

        public SnapshotExecutionImpl(String name, String snapshotId,
                                     List<ExecutionArg> output,
                                     List<InputExecutionArg> input) {
            this.name = name;
            this.snapshot = snapshotId;
            this.output = output;
            this.input = input;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Stream<ExecutionArg> outputArgs() {
            return output.stream();
        }

        @Override
        public Stream<InputExecutionArg> inputArgs() {
            return input.stream();
        }

        @Override
        public String snapshotId() {
            return snapshot;
        }
    }

    class ExecutionArgImpl implements ExecutionArg {
        private final String name;
        private final String snapshotId;
        private final String entryId;

        public ExecutionArgImpl(String name, String snapshotId, String entryId) {
            this.name = name;
            this.snapshotId = snapshotId;
            this.entryId = entryId;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String snapshotId() {
            return snapshotId;
        }

        @Override
        public String entryId() {
            return entryId;
        }
    }

    class InputExecutionArgImpl extends ExecutionArgImpl implements InputExecutionArg {
        private final String hash;

        public InputExecutionArgImpl(String name, String snapshotId, String entryId, String hash) {
            super(name, snapshotId, entryId);
            this.hash = hash;
        }

        @Override
        public String hash() {
            return hash;
        }
    }
}
