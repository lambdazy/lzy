package ru.yandex.qe.s3.transfer.meta;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Established by terry on 18.01.16.
 */
@NotThreadSafe
@Immutable
public class Metadata {

    public static final long UNDEFINED_LENGTH = -1;

    private Map<String, Object> metadata;
    private Map<String, Object> userMetadata;

    private List<Object> aclObjects;

    private long objectContentLength = UNDEFINED_LENGTH;

    public Metadata(Map<String, Object> metadata, Map<String, Object> userMetadata, List<Object> aclObjects,
        long objectContentLength) {
        this.metadata = metadata;
        this.userMetadata = userMetadata;
        this.aclObjects = aclObjects;
        this.objectContentLength = objectContentLength;
    }

    public static Metadata empty() {
        return new Metadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), UNDEFINED_LENGTH);
    }

    public Map<String, Object> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    public Map<String, Object> getUserMetadata() {
        return Collections.unmodifiableMap(userMetadata);
    }

    public List<Object> getAclObjects() {
        return Collections.unmodifiableList(aclObjects);
    }


    /**
     * @return length source object, return -1 if length not defined
     */
    public long getObjectContentLength() {
        return objectContentLength;
    }


    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override
    public Metadata clone() {
        return new MetadataBuilder()
            .addMetadata(getMetadata())
            .addUserMetadata(getUserMetadata())
            .addAclObjects(getAclObjects())
            .setObjectContentLength(getObjectContentLength()).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Metadata metadata1 = (Metadata) o;

        if (objectContentLength != metadata1.objectContentLength) {
            return false;
        }
        if (aclObjects != null ? !aclObjects.equals(metadata1.aclObjects) : metadata1.aclObjects != null) {
            return false;
        }
        if (metadata != null ? !metadata.equals(metadata1.metadata) : metadata1.metadata != null) {
            return false;
        }
        if (userMetadata != null ? !userMetadata.equals(metadata1.userMetadata) : metadata1.userMetadata != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = metadata != null ? metadata.hashCode() : 0;
        result = 31 * result + (userMetadata != null ? userMetadata.hashCode() : 0);
        result = 31 * result + (aclObjects != null ? aclObjects.hashCode() : 0);
        result = 31 * result + (int) (objectContentLength ^ (objectContentLength >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Metadata{"
            + "metadata=" + metadata
            + ", userMetadata=" + userMetadata
            + ", aclObjects=" + aclObjects
            + ", objectContentLength=" + objectContentLength
            + '}';
    }
}
