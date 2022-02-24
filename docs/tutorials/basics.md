## ʎzy basics

### Code adaptation

Just decorate your python functions with `@op` and annotate them with types. **Please note: type
annotations are required.** For example:

```python
from dataclasses import dataclass
from catboost import CatBoostClassifier

from lzy.api import op, LzyRemoteEnv, Gpu
import numpy as np


@dataclass
class DataSet:
    data: np.array
    labels: np.array


@op
def dataset() -> DataSet:
    train_data = np.array([[0, 3],
                           [4, 1],
                           [8, 1],
                           [9, 1]])
    train_labels = np.array([0, 0, 1, 1])
    return DataSet(train_data, train_labels)
```

### Running function

To run the decorated function on the cluster, you can just call it within the `LzyRemoteEnv` block:

```python
with LzyRemoteEnv():
    data = dataset()
```

You can also specify resources required by a function. Currently, only GPU requirement is supported (function will be run on the VM with 12 vCPU, 112GB RAM, and Tesla M60 GPU):

```python
@op(gpu=Gpu.any())
def learn(data_set: DataSet) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(data_set.data, data_set.labels, verbose=False)
    return cb_model
```

If resources are not specified, a function will be run on the default VM with 2 vCPU and 8GB RAM.

### Running graph

You can build and run an execution graph just using function calls:

```python
with LzyRemoteEnv():
    data = dataset()
    model = learn(data)
    result = model.predict(np.array([9, 1]))
```

The continuous sequences of remote calls form a graph that runs only when results are needed:

```python
with LzyRemoteEnv():
    data = dataset()                            # lazy call
    model = learn(data)                         # lazy call
    result = model.predict(np.array([9, 1]))    # model object is required - graph containing dataset and learn functions is started
```

You can force materialization using the `run` method:

```python
with LzyRemoteEnv() as env:
    data = dataset()
    env.run()                                   # enforce remote call of the dataset function
    model = learn(data)
    result = model.predict(np.array([9, 1]))
```

Or just enforce eager execution for the whole graph:

```python
with LzyRemoteEnv(eager=True) as env:
    ...
```

**TBD:** ʎzy runs independent functions in parallel:

```python
with LzyRemoteEnv() as env:
    data = dataset()
    params = params()
    model = learn(data, params)
    result = model.predict(np.array([9, 1]))    # dataset and params functions run in parallel 
```

---

You can find the complete code of this step [here](../../lzy-python/examples/catboost.py).

In the [**next**](environment.md) part, we will dive into the environment setup.
