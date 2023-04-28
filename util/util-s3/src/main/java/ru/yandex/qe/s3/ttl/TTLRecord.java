package ru.yandex.qe.s3.ttl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import org.joda.time.DateTime;

import java.util.Objects;

/**
 * @author nkey
 * @since 28.01.2015
 */
public class TTLRecord {

    @Nonnull
    @JsonProperty("bucket")
    private final String bucket;
    @Nonnull
    @JsonProperty("ket")
    private final String key;
    @Nonnull
    @JsonProperty("expire-at")
    private final DateTime expireAt;

    @JsonCreator
    public TTLRecord(@JsonProperty("bucket") @Nonnull String bucket,
        @JsonProperty("ket") @Nonnull String key,
        @JsonProperty("expire-at") @Nonnull DateTime expireAt) {
        this.bucket = bucket;
        this.key = key;
        this.expireAt = expireAt;
    }

    @Nonnull
    public String getBucket() {
        return bucket;
    }

    @Nonnull
    public String getKey() {
        return key;
    }

    @Nonnull
    public DateTime getExpireAt() {
        return expireAt;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, key, expireAt);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TTLRecord other = (TTLRecord) obj;
        return Objects.equals(this.bucket, other.bucket) && Objects.equals(this.key, other.key) && Objects.equals(
            this.expireAt, other.expireAt);
    }
}
