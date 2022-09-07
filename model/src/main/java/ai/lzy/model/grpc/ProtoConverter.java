package ai.lzy.model.grpc;

import ai.lzy.model.StorageCredentials;
import ai.lzy.model.basic.SlotInstance;
import ai.lzy.model.basic.SlotStatus;
import ai.lzy.model.data.DataSchema;
import ai.lzy.model.data.SchemeType;
import ai.lzy.model.graph.*;
import ai.lzy.model.slot.Slot;
import ai.lzy.v1.Operations;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ProtoConverter {

    public static Slot fromProto(Operations.Slot grpcSlot) {
        return new SlotAdapter(grpcSlot);
    }

    public static SlotStatus fromProto(Operations.SlotStatus slotStatus) {
        return new SlotStatusAdapter(slotStatus);
    }
    
    public static Provisioning fromProto(Operations.Provisioning provisioning) {
        return () -> provisioning.getTagsList().stream()
            .map(Operations.Provisioning.Tag::getTag)
            .collect(Collectors.toSet());
    }

    public static Env fromProto(Operations.EnvSpec env) {
        final BaseEnv baseEnv;
        if (env.hasBaseEnv()) {
            baseEnv = fromProto(env.getBaseEnv());
        } else {
            baseEnv = null;
        }
        final AuxEnv auxEnv;
        if (env.hasAuxEnv()) {
            auxEnv = fromProto(env.getAuxEnv());
        } else {
            auxEnv = null;
        }
        return new EnvImpl(baseEnv, auxEnv);
    }

    public static BaseEnv fromProto(Operations.BaseEnv env) {
        return new BaseEnvAdapter(env);
    }

    public static AuxEnv fromProto(Operations.AuxEnv env) {
        if (env.hasPyenv()) {
            return fromProto(env.getPyenv());
        }
        return null;
    }

    private static PythonEnv fromProto(Operations.PythonEnv env) {
        return new PythonEnvAdapter(env);
    }

    public static DataSchema fromProto(Operations.DataScheme dataScheme) {
        return DataSchema.buildDataSchema(dataScheme.getSchemeType().name(), dataScheme.getType());
    }



    public static Operations.EnvSpec toProto(Env env) {
        Operations.EnvSpec.Builder builder = Operations.EnvSpec.newBuilder();
        if (env != null) {
            if (env.baseEnv() != null) {
                builder.setBaseEnv(toProto(env.baseEnv()));
            }
            if (env.auxEnv() != null) {
                builder.setAuxEnv(toProto(env.auxEnv()));
            }
        }
        return builder.build();
    }

    public static Operations.BaseEnv toProto(BaseEnv env) {
        Operations.BaseEnv.Builder builder = Operations.BaseEnv.newBuilder();
        if (env.name() != null) {
            builder.setName(env.name());
        }
        return builder.build();
    }

    public static Operations.AuxEnv toProto(AuxEnv env) {
        Operations.AuxEnv.Builder builder = Operations.AuxEnv.newBuilder();
        if (env instanceof PythonEnv) {
            builder.setPyenv(toProto((PythonEnv) env));
        }
        return builder.build();
    }

    public static Operations.PythonEnv toProto(PythonEnv env) {
        List<Operations.LocalModule> localModules = new ArrayList<>();
        env.localModules()
            .forEach(localModule -> localModules.add(Operations.LocalModule.newBuilder()
                .setName(localModule.name())
                .setUri(localModule.uri())
                .build()));
        return Operations.PythonEnv.newBuilder()
            .setName(env.name())
            .setYaml(env.yaml())
            .addAllLocalModules(localModules)
            .build();
    }

    public static Operations.Slot toProto(Slot slot) {
        return Operations.Slot.newBuilder()
            .setName(slot.name())
            .setMedia(Operations.Slot.Media.valueOf(slot.media().name()))
            .setDirection(Operations.Slot.Direction.valueOf(slot.direction().name()))
            .setContentType(toProto(slot.contentType()))
            .build();
    }

    public static Operations.DataScheme toProto(DataSchema dataSchema) {
        return Operations.DataScheme.newBuilder()
            .setType(dataSchema.typeContent())
            .setSchemeType(toProto(dataSchema.schemeType()))
            .build();
    }

    public static Operations.SchemeType toProto(SchemeType dataSchema) {
        return Operations.SchemeType.valueOf(dataSchema.name());
    }

    public static Operations.Provisioning toProto(Provisioning provisioning) {
        return Operations.Provisioning.newBuilder()
            .addAllTags(provisioning.tags().stream()
                .map(tag -> Operations.Provisioning.Tag.newBuilder().setTag(tag).build())
                .collect(Collectors.toList()))
            .build();
    }

    private static class EnvImpl implements Env {

        private final BaseEnv baseEnv;
        private final AuxEnv auxEnv;

        public EnvImpl(BaseEnv baseEnv, AuxEnv auxEnv) {
            this.baseEnv = baseEnv;
            this.auxEnv = auxEnv;
        }

        @Override
        public BaseEnv baseEnv() {
            return baseEnv;
        }

        @Override
        public AuxEnv auxEnv() {
            return auxEnv;
        }
    }

    private static class LocalModuleAdapter implements LocalModule {

        private final String name;
        private final String uri;

        public LocalModuleAdapter(Operations.LocalModule localModule) {
            this.name = localModule.getName();
            this.uri = localModule.getUri();
        }

        public String name() {
            return name;
        }

        public String uri() {
            return uri;
        }
    }

    private static class PythonEnvAdapter implements PythonEnv {

        private final Operations.PythonEnv env;
        private final List<LocalModule> localModules;

        public PythonEnvAdapter(Operations.PythonEnv env) {
            this.env = env;
            localModules = new ArrayList<>();
            env.getLocalModulesList()
                .forEach(localModule -> localModules.add(new LocalModuleAdapter(localModule)));
        }

        @Override
        public String name() {
            return this.env.getName();
        }

        @Override
        public String yaml() {
            return this.env.getYaml();
        }

        @Override
        public List<LocalModule> localModules() {
            return new ArrayList<>(localModules);
        }

        @Nullable
        @Override
        public StorageCredentials credentials() {
            // TODO(artolord) add credentials to proto
            return null;
        }

        @Override
        public URI uri() {
            return URI.create("conda/" + name());
        }
    }

    private static class BaseEnvAdapter implements BaseEnv {

        private final Operations.BaseEnv env;

        public BaseEnvAdapter(Operations.BaseEnv env) {
            this.env = env;
        }

        @Override
        public String name() {
            if (env.getName().equals("")) {
                return null;
            }
            return env.getName();
        }
    }

    public static class SlotAdapter implements Slot {

        private final Operations.Slot s;

        public SlotAdapter(Operations.Slot s) {
            this.s = s;
        }

        @Override
        public String name() {
            return s.getName();
        }

        @Override
        public Media media() {
            return Media.valueOf(s.getMedia().name());
        }

        @Override
        public Direction direction() {
            return Direction.valueOf(s.getDirection().name());
        }

        @Override
        public @Nullable
        DataSchema contentType() {
            return fromProto(s.getContentType());
        }

        @Override
        public int hashCode() {
            return s.getName().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Slot && ((Slot) obj).name().equals(s.getName());
        }
    }

    private static class SlotStatusAdapter implements SlotStatus {

        private final Operations.SlotStatus slotStatus;

        SlotStatusAdapter(Operations.SlotStatus slotStatus) {
            this.slotStatus = slotStatus;
        }

        @Nullable
        @Override
        public String channelId() {
            return slotStatus.getConnectedTo();
        }

        @Override
        public String tid() {
            slotStatus.getTaskId();
            return !slotStatus.getTaskId().isEmpty() ? slotStatus.getTaskId() : null;
        }

        @Override
        public Slot slot() {
            return fromProto(slotStatus.getDeclaration());
        }

        @Override
        public URI connected() {
            return slotStatus.getConnectedTo().isEmpty() ? null
                : URI.create(slotStatus.getConnectedTo());
        }

        @Override
        public long pointer() {
            return slotStatus.getPointer();
        }

        @Override
        public State state() {
            return State.valueOf(slotStatus.getState().name());
        }
    }


}
