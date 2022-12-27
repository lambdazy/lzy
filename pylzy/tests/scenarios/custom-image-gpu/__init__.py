import uuid

import numpy as np
from catboost import CatBoostClassifier

from lzy.api.v1 import Lzy, op


def train(data: np.ndarray, target: np.ndarray) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(
        iterations=1000, devices="0:1", train_dir="/tmp/catboost"
    )
    cb_model.fit(data, target, verbose=True)
    return cb_model


@op(gpu=Gpu.any())
def train_default_env(data: np.ndarray, target: np.ndarray) -> CatBoostClassifier:
    return train(data, target)


@op(gpu=Gpu.any(), docker_image="lzydock/default-env:for-tests")
def train_custom_env(data: np.ndarray, target: np.ndarray) -> CatBoostClassifier:
    return train(data, target)


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == "__main__":
    lzy = Lzy()
    WORKFLOW_NAME = "workflow-" + str(uuid.uuid4())

    data = np.array([[0, 3], [4, 1], [8, 1], [9, 1]])
    labels = np.array([0, 0, 1, 1])

    with lzy.workflow(name=WORKFLOW_NAME):
        model = train_custom_env(data, labels)
        result = model.predict(np.array([0, 3]))
        print("Prediction: " + str(result))

    with lzy.workflow(name=WORKFLOW_NAME):
        model = train_default_env(data, labels)
        result = model.predict(np.array([9, 1]))
        print("Prediction: " + str(result))
