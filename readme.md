[![Pypi version](https://img.shields.io/pypi/v/pylzy)](https://pypi.org/project/pylzy/)
[![Tests](https://github.com/lambda-zy/lzy/actions/workflows/pull-tests.yaml/badge.svg)](https://github.com/lambda-zy/lzy/actions/workflows/pull-tests.yaml)
[![Java tests coverage](https://gist.githubusercontent.com/mrMakaronka/be651155cb12a8006cecdee948ce1a0a/raw/master-java-coverage.svg)]()
[![Python tests coverage](https://gist.githubusercontent.com/mrMakaronka/0095e900fb0fcbe5575ddc3c717fb65b/raw/master-coverage.svg)](https://github.com/lambdazy/lzy/tree/master/pylzy/tests)

# ʎzy

ʎzy is a platform for a hybrid execution of ML workflows that transparently integrates local and remote runtimes 
with the following properties:

- Python-native SDK
- Automatic env (pip/conda) sync
- K8s-native runtime
- Resources allocation on-demand
- Env-independent results storage

## Quick start

[![Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1Z7CERGqTU-ZTu3dwbeZxD9zJ6L8oQBbN?usp=sharing)

ʎzy allows running any python functions on a cluster by annotating them with `@op` decorator:

```python
@op(gpu=Gpu.any())
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model


# local python function call
model = train(data_set)

# remote call on a cluster
env = LzyRemoteEnv()
with env.workflow("training"):
    model = train(data_set)
```

Please read the [tutorial](docs/tutorials/0-contents.md) for details. We provide a free [sandbox installation](https://lzy.ai).

## Runtime

Check out our [key concepts](docs/arch/key-concepts.md) and [architecture intro](docs/arch/intro_en.md).

## Development

Development [guide](docs/development.md).

## Deployment

Deployment guide.

* [Azure](docs/deployment_azure.md)
* [YCloud](docs/deployment_ycloud.md)
