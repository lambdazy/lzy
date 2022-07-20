from catboost import CatBoostClassifier
import uuid
import numpy as np
from lzy.api.v1 import LzyRemoteEnv, op


def train(data: np.ndarray, target: np.ndarray) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices="0:1", train_dir="/tmp/catboost")
    cb_model.fit(data, target, verbose=True)
    return cb_model


@op
def train_default_env(data: np.ndarray, target: np.ndarray) -> CatBoostClassifier:
    return train(data, target)


@op(docker_image="lzydock/default-env:for-tests")
def train_custom_env(data: np.ndarray, target: np.ndarray) -> CatBoostClassifier:
    return train(data, target)


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())


if __name__ == "__main__":
    data = np.array([[0, 3], [4, 1], [8, 1], [9, 1]])
    labels = np.array([0, 0, 1, 1])

    with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
        model = train_custom_env(data, labels)
        result = model.predict(np.array([0, 3]))
        print("Prediction: " + str(result))

    with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
        model = train_default_env(data, labels)
        result = model.predict(np.array([9, 1]))
        print("Prediction: " + str(result))
