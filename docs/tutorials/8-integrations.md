## ʎzy integrations

### Catboost

ʎzy has a native integration with [catboost](https://catboost.ai) - an open source library for gradient boosting on
decision trees. Training is transparently run in cloud if the `provisioning` param is specified for the `fit` method:

```python
from catboost import CatBoostClassifier
from sklearn import datasets
from lzy.api.v1 import Provisioning, Gpu

data_set = datasets.load_breast_cancer()

model = CatBoostClassifier(iterations=1000, train_dir='/tmp/catboost')
model.fit(data_set.data, data_set.target,
          provisioning=Provisioning(gpu=Gpu.any()))  # local catboost call without `provisioning` param

result = model.predict(data_set.data[0])
print(result)
```