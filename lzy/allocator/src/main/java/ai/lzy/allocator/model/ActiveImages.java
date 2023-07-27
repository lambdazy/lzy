package ai.lzy.allocator.model;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ActiveImages {

    public record WorkerImage(
        String image
    ) {
        public static WorkerImage of(String image) {
            return new WorkerImage(image);
        }
    }

    public record SyncImage(
        String image
    ) {
        public static SyncImage of(String image) {
            return new SyncImage(image);
        }
    }

    public record JupyterLabImage(
        String mainImage,
        String[] additionalImages
    ) {
        public static JupyterLabImage of(String image, String[] additional) {
            return new JupyterLabImage(image, additional);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            var that = (JupyterLabImage) o;
            return Objects.equals(mainImage, that.mainImage) && Arrays.equals(additionalImages, that.additionalImages);
        }

        @Override
        public String toString() {
            return "JupyterLabImage{" +
                "mainImage='" + mainImage + '\'' +
                ", additionalImages=" + Arrays.toString(additionalImages) +
                '}';
        }
    }

    public record Configuration(
        SyncImage sync,
        List<WorkerImage> workers,
        List<JupyterLabImage> jupyterLabs
    ) {
        public boolean isEmpty() {
            return sync.image() == null || workers.isEmpty();
        }
    }

}
