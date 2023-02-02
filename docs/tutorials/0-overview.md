## Overview

ʎzy is a platform for a hybrid execution of ML workflows that transparently integrates local and remote runtimes
with the following properties:

- Python-native SDK
- Automatic env (pip/conda) sync
- K8s-native runtime
- Resources allocation on-demand
- Env-independent results storage

### Quick start

ʎzy allows running any python functions on a cluster by annotating them with `@op` decorator:

```python
@op(gpu_count=1, gpu_type=GpuType.V100.name)
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model


# local python function call
model = train(data_set)

# remote call on a cluster
lzy = Lzy()
with lzy.workflow("training"):
    model = train(data_set)
```

Please read the [tutorial](1-setup.md) for details:

1. [Setup](1-setup.md)
2. [Auth](2-auth.md)
2. [Basics](3-basics.md)
2. [Data transfer](4-data.md)
2. [Dev Environment](5-environment.md)
2. [Managing Graph Results - Whiteboards](6-whiteboards.md)
2. [Integrations (catboost native)](7-integrations.md)