import numpy as np
from catboost import CatBoostClassifier
import uuid

from lzy.api import op, LzyRemoteEnv, Gpu

'''
This scenario contains:
    1. Importing external modules (catboost)
    2. Functions which accept and return complex objects
    3. Task that requires GPU
    4. Keyed arguments
'''


@op(gpu=Gpu.any())
def learn() -> CatBoostClassifier:
    train_data = np.array([[0, 3],
                           [4, 1],
                           [8, 1],
                           [9, 1]])
    train_labels = np.array([0, 0, 1, 1])
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(train_data, train_labels, verbose=False)
    return cb_model


@op
def predict(cb_model: CatBoostClassifier, point: np.array) -> np.int64:
    return cb_model.predict(point)


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == '__main__':
    with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
        model = learn()
        result = predict(model, point=np.array([9, 1]))
        print("Prediction: " + str(result))
