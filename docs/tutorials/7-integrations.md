## ʎzy integrations

### Catboost

ʎzy has a native integration with [catboost](https://catboost.ai) - an open source library for gradient boosting on
decision trees. Training is transparently run in cloud if the extra provisioning parameters (e.g., `gpu_type` and
`gpu_count`) are specified for the `fit` method:

```python
from catboost import CatBoostClassifier
from sklearn import datasets
from lzy.api.v1 import Provisioning, GpuType
from lzy.injections.catboost import inject_catboost

inject_catboost()
data_set = datasets.load_breast_cancer()

model = CatBoostClassifier(iterations=1000, train_dir='/tmp/catboost')
model.fit(data_set.data, data_set.target,
          gpu_type=GpuType.V100.name, gpu_count=1)  # local catboost call without these extra param

result = model.predict(data_set.data[0])
print(result)
```