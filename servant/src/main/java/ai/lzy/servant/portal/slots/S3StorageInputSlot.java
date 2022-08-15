package ai.lzy.servant.portal.slots;

import ai.lzy.fs.slots.LzyInputSlotBase;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.s3.S3Repository;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.stream.Stream;

import static ai.lzy.v1.Operations.SlotStatus.State.OPEN;

public class S3StorageInputSlot extends LzyInputSlotBase {
    private static final Logger LOG = LogManager.getLogger(S3StorageInputSlot.class);

    private final String key;
    private final String bucket;
    private final S3Repository<Stream<ByteString>> repository;

    public S3StorageInputSlot(SlotInstance instance, String s3Key, String s3Bucket,
                              S3Repository<Stream<ByteString>> s3Repository) {
        super(instance);
        this.key = s3Key;
        this.bucket = s3Bucket;
        this.repository = s3Repository;
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> inputData) {
        super.connect(slotUri, inputData);
        LOG.info("Attempt to connect slot {} to {}", name(), slotUri);
        repository.put(bucket, key, inputData);
        state(OPEN);
    }
}
