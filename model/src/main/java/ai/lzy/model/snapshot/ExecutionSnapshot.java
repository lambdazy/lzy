package ai.lzy.model.snapshot;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface ExecutionSnapshot {

    String name();

    Stream<ExecutionValue> outputs();

    Stream<InputExecutionValue> inputs();

    String snapshotId();

    class ExecutionSnapshotImpl implements ExecutionSnapshot {

        private final String name;
        private final String snapshotId;
        private final List<ExecutionValue> outputs;
        private final List<InputExecutionValue> inputs;

        public ExecutionSnapshotImpl(String name, String snapshotId,
            List<ExecutionValue> outputs,
            List<InputExecutionValue> inputs) {
            this.name = name;
            this.snapshotId = snapshotId;
            this.outputs = outputs;
            this.inputs = inputs;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Stream<ExecutionValue> outputs() {
            return outputs.stream();
        }

        @Override
        public Stream<InputExecutionValue> inputs() {
            return inputs.stream();
        }

        @Override
        public String snapshotId() {
            return snapshotId;
        }

        public String toString() {
            return "name: " + name + ", snapshot id: " + snapshotId + ", outputs: {" + Arrays.toString(
                outputs.toArray()) + "}" + ", inputs: {" + Arrays.toString(inputs.toArray()) + "}";
        }
    }

    class ExecutionValueImpl implements ExecutionValue {

        private final String name;
        private final String snapshotId;
        private final String entryId;

        public ExecutionValueImpl(String name, String snapshotId, String entryId) {
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

    class InputExecutionValueImpl extends ExecutionValueImpl implements InputExecutionValue {

        private final String hash;

        public InputExecutionValueImpl(String name, String snapshotId, String entryId, String hash) {
            super(name, snapshotId, entryId);
            this.hash = hash;
        }

        @Override
        public String hash() {
            return hash;
        }
    }
}
