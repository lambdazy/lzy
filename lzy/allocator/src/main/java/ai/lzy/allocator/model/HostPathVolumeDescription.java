package ai.lzy.allocator.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class HostPathVolumeDescription extends VolumeRequest.VolumeDescription {
    private final String name;
    private final String path;
    private final HostPathType hostPathType;

    @JsonCreator
    public HostPathVolumeDescription(@JsonProperty("name") String name,
                                     @JsonProperty("path") String path,
                                     @JsonProperty("host_path_type") HostPathType hostPathType)
    {
        this.name = name;
        this.path = path;
        this.hostPathType = hostPathType;
    }

    public enum HostPathType {
        DIRECTORY_OR_CREATE("DirectoryOrCreate"),
        DIRECTORY("Directory"),
        FILE_OR_CREATE("FileOrCreate"),
        FILE("File"),
        SOCKET("Socket");

        private final String repr;

        HostPathType(String strRepr) {
            repr = strRepr;
        }

        public String asString() {
            return repr;
        }
    }

    @Override
    public String name() {
        return name;
    }

    public String path() {
        return path;
    }

    public HostPathType hostPathType() {
        return hostPathType;
    }

    @Override
    public String toString() {
        return "HostPathVolumeDescription{" +
            ", name='" + name + '\'' +
            ", path='" + path + '\'' +
            ", hostPathType=" + hostPathType +
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
        HostPathVolumeDescription that = (HostPathVolumeDescription) o;
        return Objects.equals(name, that.name)
            && Objects.equals(path, that.path)
            && hostPathType == that.hostPathType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path, hostPathType);
    }
}
