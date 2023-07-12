import pytest

from lzy.env.container.base import BaseContainer, ContainerTypes, DockerProtocol, DockerPullPolicy


def test_inheritance():
    with pytest.raises(TypeError, match=r".*must have container_type attribute"):
        class BadClass(BaseContainer):
            pass

    with pytest.raises(TypeError, match=rf".*must be realization of {DockerProtocol}"):
        class BadClass2(BaseContainer):
            container_type = ContainerTypes.Docker

    # no raise
    class GoodClass(BaseContainer):
        container_type = ContainerTypes.Docker

        def get_image_url(self):
            return ''

        def get_pull_policy(self):
            return DockerPullPolicy.ALWAYS

        def get_username(self):
            pass

        def get_password(self):
            pass
