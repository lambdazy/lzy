[![Tests](https://github.com/lambda-zy/lzy/actions/workflows/pull-tests.yaml/badge.svg)](https://github.com/lambda-zy/lzy/actions/workflows/pull-tests.yaml)

# ʎzy

ʎzy is a system for the distributed execution of an arbitrary code and storage of the obtained results.

The goals of this system are:
- Transparent scaling of code that is not generally intended for distributed execution
- Run ML training and inference tasks in one computing environment, effectively balancing the load between these circuits.
- Provide an ability to combine local and distributed components in one task.
- Allow an ML specialist to implement an arbitrary configuration of the computing environment (MR, main-secondary, rings/trees, etc.)

The system is based on the following principle: a computing cluster is represented as one large UNIX machine. Calculations communicate with each other using shared files, pipes, and other familiar machinery. The user controls this large UNIX machine either from his local laptop, where the system crawls through a partition in the file system.

## Quick start

ʎzy allows running any python functions in the Cloud by annotating them with `@op` decorator:

```python
@op(gpu=Gpu.any())
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model
```

Please read the [tutorial](docs/tutorials/1-setup.md) for details.

## Development

Development [guide](docs/development.md).

## Deployment

Deployment [guide](docs/deployment.md).
