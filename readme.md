![](https://github.com/lambda-zy/lzy/actions/workflows/pull-request-workflow.yaml/badge.svg)
![](https://github.com/lambda-zy/lzy/actions/workflows/acceptance-tests-cron.yaml/badge.svg)

# ʎzy

ʎzy is a system for the distributed execution of an arbitrary code and storage of the obtained results.

We believe that the goals of this system are:
- Transparent scaling of code that is not generally intended for distributed execution
- Run ML training and inference tasks in one computing environment, effectively balancing the load between these circuits.
- Provide an ability to combine local and distributed components in one task.
- Allow an ML specialist to implement an arbitrary configuration of the computing environment (MR, main-secondary, rings/trees, etc.)

The system is based on the following principle: a computing cluster is represented as one large UNIX machine. Calculations communicate with each other using shared files, pipes, and other familiar machinery. The user controls this large UNIX machine either from his local laptop, where the system crawls through a partition in the file system.

## Quick start

### Installation

`pip install pylzy-nightly`

### Generating keys
To access lzy, you must generate RSA public and private key, and then add them to [site](http://lzy.northeurope.cloudapp.azure.com/add_token).

To generate RSA keys with openssl, run commands:
```shell
$ openssl genrsa -out ~/.ssh/private.pem 2048
$ openssl rsa -in ~/.ssh/private.pem -outform PEM -pubout -out ~/.ssh/public.pem
```

Then copy content of `~/.ssh/public.pem` to form on [site](http://lzy.northeurope.cloudapp.azure.com/add_token).

### Running

Just decorate your python functions with `@op` and run them within `LzyEnv` block!

```python
from dataclasses import dataclass
from catboost import CatBoostClassifier
from lzy.api import op, LzyEnv, Gpu
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


@op(gpu=Gpu.any())
def learn(data_set: DataSet) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="GPU", devices='0:1')
    cb_model.fit(data_set.data, data_set.labels, verbose=False)
    return cb_model


@op
def predict(cb_model: CatBoostClassifier, point: np.array) -> np.int64:
    return cb_model.predict(point)


if __name__ == '__main__':
    with LzyEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem"):
        data = dataset()
        model = learn(data)
        result = predict(model, np.array([9, 1]))
    print(result)

```

## Development

### Before local run

* For macOS: install [macFuse](https://osxfuse.github.io)

* For arch linux:
```
pacman -Sy fuse2 inetutils
```

### Local run

1. Run [Server](lzy-server/readme.md)
2. Run [Kharon](lzy-kharon/readme.md)
3. Run [Terminal](lzy-servant/readme.md)
4. Now ʎzy FS should be available at path `/tmp/lzy`
---
**For python API:**

5. Install [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
6. `cd lzy-servant/ && ./prepare_envs.sh ../lzy-python` (conda envs setup)

### FAQ

* ```Exception in thread "main" java.lang.UnsatisfiedLinkError: dlopen(libfuse.dylib, 9): image not found```: Java > 11 is required
