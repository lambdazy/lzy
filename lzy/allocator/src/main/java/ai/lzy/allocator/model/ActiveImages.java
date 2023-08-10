package ai.lzy.allocator.model;

import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class ActiveImages {

    public record Image(
        String image
    )
    {
        public static Image of(String image) {
            return new Image(image);
        }
    }

    public record DindImages(
        String dindImage,
        List<String> additionalImages
    )
    {
        public static DindImages of(String image, List<String> additional) {
            return new DindImages(image, additional);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            var that = (DindImages) o;
            return Objects.equals(dindImage, that.dindImage) && additionalImages.equals(that.additionalImages);
        }

        @Override
        public String toString() {
            return "DinDImage{" +
                "dindSyncImage='" + dindImage + '\'' +
                ", additionalImages=" + additionalImages +
                '}';
        }
    }

    public record PoolConfig(
        List<Image> images,
        @Nullable DindImages dindImages,
        String kind,
        String name
    )
    {
        public static PoolConfig of(List<Image> images, DindImages dindImages, String kind, String name) {
            return new PoolConfig(images, dindImages, kind, name);
        }
    }

    public record Configuration(
        Image sync,
        List<PoolConfig> configs
    )
    {
        public List<PoolConfig> byLabels(String kind, String name) {
            return configs.stream().filter(poolConfig -> poolConfig.kind.equals(kind) && poolConfig.name.equals(name))
                .toList();
        }

        public boolean isEmpty() {
            return sync.image() == null || configs.isEmpty();
        }
    }

}
