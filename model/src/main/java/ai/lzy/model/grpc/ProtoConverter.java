package ai.lzy.model.grpc;

import ai.lzy.model.DataScheme;
import ai.lzy.model.StorageCredentials;
import ai.lzy.model.graph.*;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.model.slot.SlotStatus;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMS;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ProtoConverter {

    public static Slot fromProto(LMS.Slot grpcSlot) {
        return new SlotAdapter(grpcSlot);
    }

    public static SlotStatus fromProto(LMS.SlotStatus slotStatus) {
        return new SlotStatusAdapter(slotStatus);
    }

    public static SlotInstance fromProto(LMS.SlotInstance slotInstance) {
        return new SlotInstance(
            fromProto(slotInstance.getSlot()),
            slotInstance.getTaskId(),
            slotInstance.getChannelId(),
            URI.create(slotInstance.getSlotUri())
        );
    }

    public static Provisioning fromProto(LME.Provisioning provisioning) {
        return () -> provisioning.getTagsList().stream()
            .map(LME.Provisioning.Tag::getTag)
            .collect(Collectors.toSet());
    }

    public static Env fromProto(LME.EnvSpec env) {
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

    public static BaseEnv fromProto(LME.BaseEnv env) {
        return new BaseEnvAdapter(env);
    }

    public static AuxEnv fromProto(LME.AuxEnv env) {
        if (env.hasPyenv()) {
            return fromProto(env.getPyenv());
        }
        return null;
    }

    private static PythonEnv fromProto(LME.PythonEnv env) {
        return new PythonEnvAdapter(env);
    }

    public static DataScheme fromProto(LMD.DataScheme dataScheme) {
        return new DataScheme(dataScheme.getDataFormat(), dataScheme.getSchemeFormat(),
            dataScheme.getSchemeContent(), dataScheme.getMetadataMap());
    }

    public static LME.EnvSpec toProto(Env env) {
        LME.EnvSpec.Builder builder = LME.EnvSpec.newBuilder();
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

    public static LME.BaseEnv toProto(BaseEnv env) {
        LME.BaseEnv.Builder builder = LME.BaseEnv.newBuilder();
        if (env.name() != null) {
            builder.setName(env.name());
        }
        return builder.build();
    }

    public static LME.AuxEnv toProto(AuxEnv env) {
        LME.AuxEnv.Builder builder = LME.AuxEnv.newBuilder();
        if (env instanceof PythonEnv) {
            builder.setPyenv(toProto((PythonEnv) env));
        }
        return builder.build();
    }

    public static LME.PythonEnv toProto(PythonEnv env) {
        List<LME.LocalModule> localModules = new ArrayList<>();
        env.localModules()
            .forEach(localModule -> localModules.add(LME.LocalModule.newBuilder()
                .setName(localModule.name())
                .setUri(localModule.uri())
                .build()));
        return LME.PythonEnv.newBuilder()
            .setName(env.name())
            .setYaml(env.yaml())
            .addAllLocalModules(localModules)
            .build();
    }

    public static LMS.Slot toProto(Slot slot) {
        return LMS.Slot.newBuilder()
            .setName(slot.name())
            .setMedia(LMS.Slot.Media.valueOf(slot.media().name()))
            .setDirection(LMS.Slot.Direction.valueOf(slot.direction().name()))
            .setContentType(toProto(slot.contentType()))
            .build();
    }

    public static LMD.DataScheme toProto(DataScheme dataScheme) {
        return LMD.DataScheme.newBuilder()
            .setDataFormat(dataScheme.dataFormat())
            .setSchemeFormat(dataScheme.schemeFormat())
            .setSchemeContent(dataScheme.schemeContent())
            .putAllMetadata(dataScheme.metadata())
            .build();
    }

    public static LME.Provisioning toProto(Provisioning provisioning) {
        return LME.Provisioning.newBuilder()
            .addAllTags(provisioning.tags().stream()
                .map(tag -> LME.Provisioning.Tag.newBuilder().setTag(tag).build())
                .collect(Collectors.toList()))
            .build();
    }

    public static LMS.SlotInstance toProto(SlotInstance slotInstance) {
        return LMS.SlotInstance.newBuilder()
            .setSlot(ProtoConverter.toProto(slotInstance.spec()))
            .setTaskId(slotInstance.taskId())
            .setChannelId(slotInstance.channelId())
            .setSlotUri(slotInstance.uri().toString())
            .build();
    }

    public static LMS.Slot buildInputPlainContentSlot(String slotName) {
        return buildInputSlot(slotName, toProto(DataScheme.PLAIN));
    }

    public static LMS.Slot buildOutputPlainContentSlot(String slotName) {
        return buildOutputSlot(slotName, toProto(DataScheme.PLAIN));
    }

    public static LMS.Slot buildInputSlot(String slotName, LMD.DataScheme scheme) {
        return buildSlot(slotName, LMS.Slot.Direction.INPUT, scheme);
    }

    public static LMS.Slot buildOutputSlot(String slotName, LMD.DataScheme scheme) {
        return buildSlot(slotName, LMS.Slot.Direction.OUTPUT, scheme);
    }

    public static LMS.Slot buildSlot(String slotName, LMS.Slot.Direction direction, LMD.DataScheme scheme) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setDirection(direction)
            .setContentType(scheme)
            .setMedia(LMS.Slot.Media.FILE)
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

        public LocalModuleAdapter(LME.LocalModule localModule) {
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

        private final LME.PythonEnv env;
        private final List<LocalModule> localModules;

        public PythonEnvAdapter(LME.PythonEnv env) {
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

        private final LME.BaseEnv env;

        public BaseEnvAdapter(LME.BaseEnv env) {
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

        private final LMS.Slot s;

        public SlotAdapter(LMS.Slot s) {
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
        DataScheme contentType() {
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

        private final LMS.SlotStatus slotStatus;

        SlotStatusAdapter(LMS.SlotStatus slotStatus) {
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
