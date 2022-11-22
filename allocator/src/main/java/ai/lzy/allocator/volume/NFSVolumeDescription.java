package ai.lzy.allocator.volume;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class NFSVolumeDescription extends VolumeRequest.ResourceVolumeDescription {
    private final String id;
    private final String name;
    private final String server;
    private final String share;
    private final int capacity;

    private final List<String> mountOptions;

    @JsonCreator
    public NFSVolumeDescription(@JsonProperty("id") String id,
                                @JsonProperty("name") String name,
                                @JsonProperty("server") String server,
                                @JsonProperty("share") String share,
                                @JsonProperty("capacity") int capacity,
                                @JsonProperty("mount_options") List<String> mountOptions)
    {
        this.id = id;
        this.name = name;
        this.server = server;
        this.share = share;
        this.capacity = capacity;
        this.mountOptions = mountOptions;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    public String share() {
        return share;
    }

    public int capacity() {
        return capacity;
    }

    public String server() {
        return server;
    }

    public List<String> mountOptions() {
        return mountOptions;
    }

    @Override
    public String toString() {
        return "HostPathVolumeDescription{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", path='" + share + '\'' +
                ", capacity='" + capacity + '\'' +
                ", server='" + server + '\'' +
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
        return Objects.equals(id, that.id)
            && Objects.equals(server, that.server)
            && Objects.equals(share, that.share)
            && Objects.equals(capacity, that.capacity)
            && mountOptions.equals(that.mountOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, server, share, capacity, mountOptions);
    }
}
