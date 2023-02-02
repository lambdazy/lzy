## ʎzy basics

### Code adaptation

Just decorate your python functions with `@op` and annotate them with types. **Please note: type
annotations are required.** For example:

```python
from sklearn import datasets
from sklearn.utils import Bunch

from catboost import CatBoostClassifier
from lzy.api.v1 import op, Lzy, GpuType

@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set
```

### Running function

To run the decorated function on the cluster, you can call it within the `workflow` block:

```python
lzy = Lzy()
with lzy.workflow("data-loading"):
    data_set = dataset()
print(data_set.DESCR)
```

You can also specify resources required by a function. Currently, only GPU requirement is supported (function will be run on the VM with 8 vCPU, 48GB RAM, and Tesla V100 GPU):

```python
@op(gpu_count=1, gpu_type=GpuType.V100.name)
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model
```

If resources are not specified, a function will be run on the default VM with 4 vCPU and 32GB RAM.

### Running graph

You can build and run an execution graph using function calls:

```python
lzy = Lzy()
with lzy.workflow("training"):
    data_set = dataset()
    model = train(data_set)
```

The continuous sequences of remote calls form a graph that runs only when results are needed:

```python
lzy = Lzy()
with lzy.workflow("training"):
    data_set = dataset()                        # lazy call
    model = train(data_set)                     # lazy call
    result = model.predict(data_set.data[0])    # model object is required - graph containing dataset and learn functions is started
    print(result)
```

You can force materialization using the `run` method:

```python
lzy = Lzy()
with lzy.workflow("training") as wf:
    data_set = dataset()
    wf.barrier()                                    # enforce remote call of the dataset function
    model = train(data_set)
    result = model.predict(data_set.data[0])
    print(result)
```

Or enforce eager execution for the whole graph:

```python
lzy = Lzy()
with lzy.workflow("training", eager=True):
    ...
```

---

In the [**next**](4-data.md) part, we will touch the data transfer.
