package ai.lzy.fs.mock;

import ai.lzy.model.SlotInstance;
import com.google.protobuf.ByteString;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.model.Slot;
import ai.lzy.model.data.DataSchema;
import ai.lzy.v1.Operations;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class OutputSlotMock implements LzyOutputSlot {
    private final String name;
    private final Runnable onSuspend;
    private final Runnable onDestroy;
    private final Runnable onClose;
    private final ArrayList<Consumer<ByteString>> consumers = new ArrayList<>();
    private final HashMap<Operations.SlotStatus.State, List<Runnable>> stateTrackers = new HashMap<>();
    private Operations.SlotStatus.State state = Operations.SlotStatus.State.UNBOUND;

    OutputSlotMock(String name, Runnable onSuspend, Runnable onDestroy, Runnable onClose) {
        this.name = name;
        this.onSuspend = onSuspend;
        this.onDestroy = onDestroy;
        this.onClose = onClose;
    }

    @Override
    public SlotInstance instance() {
        try {
            return new SlotInstance(definition(), "taskId", "channelId", new URI("scheme", "host", "/path", null));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<ByteString> readFromPosition(long offset) throws IOException {
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Slot definition() {
        return new Slot() {
            @Override
            public String name() {
                return "mock";
            }

            @Override
            public Media media() {
                return null;
            }

            @Override
            public Direction direction() {
                return null;
            }

            @Override
            public DataSchema contentType() {
                return DataSchema.plain;
            }
        };
    }

    @Override
    public void suspend() {
        onSuspend.run();
    }

    @Override
    public void destroy() {
        onDestroy.run();
    }

    @Override
    public void close() {
        onClose.run();
    }

    @Override
    public Operations.SlotStatus.State state() {
        return state;
    }

    public void state(Operations.SlotStatus.State state) {
        this.state = state;
        stateTrackers.getOrDefault(state, List.of()).forEach(Runnable::run);
    }

    @Override
    public void onState(Operations.SlotStatus.State state, Runnable action) {
        stateTrackers.compute(state, (k, v) -> {
            if (v == null) {
                return List.of(action);
            }
            v.add(action);
            return v;
        });
    }

    @Override
    public void onState(Set<Operations.SlotStatus.State> state, StateChangeAction action) {
        state.forEach(state1 -> onState(state1, action));
    }

    @Override
    public Operations.SlotStatus status() {
        return null;
    }

    @Override
    public void onChunk(Consumer<ByteString> trafficTracker) {
        consumers.add(trafficTracker);
    }

    public void chunk(ByteString string) {
        consumers.forEach(t -> t.accept(string));
    }

    public static class OutputSlotMockBuilder {
        private String name;
        private Runnable onSuspend;
        private Runnable onDestroy;
        private Runnable onClose;

        public OutputSlotMockBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public OutputSlotMockBuilder setOnSuspend(Runnable onSuspend) {
            this.onSuspend = onSuspend;
            return this;
        }

        public OutputSlotMockBuilder setOnDestroy(Runnable onDestroy) {
            this.onDestroy = onDestroy;
            return this;
        }

        public OutputSlotMockBuilder setOnClose(Runnable onClose) {
            this.onClose = onClose;
            return this;
        }

        public OutputSlotMock build() {
            return new OutputSlotMock(name, onSuspend, onDestroy, onClose);
        }
    }
}
