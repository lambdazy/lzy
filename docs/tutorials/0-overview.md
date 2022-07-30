## Overview

ʎzy is a system for the distributed execution of an arbitrary code and storage of the obtained results.

The goals of the system are:

- Transparent scaling of code that is not generally intended for distributed execution
- Run ML training and inference tasks in one computing environment, effectively balancing the load between these
  circuits.
- Provide an ability to combine local and distributed components in one task.
- Allow an ML specialist to implement an arbitrary configuration of the computing environment (MR, main-secondary,
  rings/trees, etc.)

### Quick start

ʎzy allows running any python functions in the Cloud by annotating them with `@op` decorator:

```python
@op(gpu=Gpu.any())
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model


# local python function call
model = train(data_set)

# `train` function runs in the Cloud
env = LzyRemoteEnv()
with env.workflow("training"):
    model = train(data_set)
```

Please read the [tutorial](1-setup.md) for details:

1. [Setup](1-setup.md)
2. [Basics](2-basics.md)
2. [Dev Environment](3-environment.md)
2. [Data Caching](4-cache.md)
2. [Managing Graph Results - Whiteboards](5-whiteboards.md)
2. [Managing Graph Results - Views](6-views.md)
2. [Integrations (catboost native)](7-integrations.md)