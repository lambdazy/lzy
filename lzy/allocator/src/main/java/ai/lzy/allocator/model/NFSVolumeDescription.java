package ai.lzy.allocator.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class NFSVolumeDescription extends VolumeRequest.ResourceVolumeDescription {
    private final String name;
    private final String server;
    private final String share;
    private final boolean readOnly;

    private final List<String> mountOptions;

    @JsonCreator
    public NFSVolumeDescription(@JsonProperty("name") String name,
                                @JsonProperty("server") String server,
                                @JsonProperty("share") String share,
                                @JsonProperty("read_only") boolean readOnly,
                                @JsonProperty("mount_options") List<String> mountOptions)
    {
        this.name = name;
        this.server = server;
        this.share = share;
        this.readOnly = readOnly;
        this.mountOptions = mountOptions;
    }

    @Override
    public String name() {
        return name;
    }

    public String share() {
        return share;
    }

    public String server() {
        return server;
    }

    public boolean readOnly() {
        return readOnly;
    }

    public List<String> mountOptions() {
        return mountOptions;
    }

    @Override
    public String toString() {
        return "HostPathVolumeDescription{" +
            ", name='" + name + '\'' +
            ", path='" + share + '\'' +
            ", server='" + server + '\'' +
            ", readOnly=" + readOnly +
            ", options=" + mountOptions +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NFSVolumeDescription that = (NFSVolumeDescription) o;
        return Objects.equals(server, that.server)
            && Objects.equals(share, that.share)
            && Objects.equals(readOnly, that.readOnly)
            && mountOptions.equals(that.mountOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(server, share, readOnly, mountOptions);
    }
}
