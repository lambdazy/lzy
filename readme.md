[![Tests](https://github.com/lambda-zy/lzy/actions/workflows/pull-tests.yaml/badge.svg)](https://github.com/lambda-zy/lzy/actions/workflows/pull-tests.yaml)
[![Cluster tests](https://github.com/lambda-zy/lzy/actions/workflows/acceptance-tests-cron.yaml/badge.svg)](https://github.com/lambda-zy/lzy/actions/workflows/acceptance-tests-cron.yaml)

# ʎzy

ʎzy is a system for the distributed execution of an arbitrary code and storage of the obtained results.

We believe that the goals of this system are:
- Transparent scaling of code that is not generally intended for distributed execution
- Run ML training and inference tasks in one computing environment, effectively balancing the load between these circuits.
- Provide an ability to combine local and distributed components in one task.
- Allow an ML specialist to implement an arbitrary configuration of the computing environment (MR, main-secondary, rings/trees, etc.)

The system is based on the following principle: a computing cluster is represented as one large UNIX machine. Calculations communicate with each other using shared files, pipes, and other familiar machinery. The user controls this large UNIX machine either from his local laptop, where the system crawls through a partition in the file system.

## Quick start

### Requirements
* fuse ([macFuse](https://osxfuse.github.io) for macos)
* java version >= 11

### Installation

`pip install pylzy` or 

`pip install pylzy-nightly` for preview version

### Generating keys
To access lzy, you must generate RSA public and private key, and then add them to [site](http://lzy.ai/keys).

To generate RSA keys with openssl, run commands:
```shell
$ openssl genrsa -out ~/.ssh/private.pem 2048
$ openssl rsa -in ~/.ssh/private.pem -outform PEM -pubout -out ~/.ssh/public.pem
```

Then copy content of `~/.ssh/public.pem` to form on [site](http://lzy.ai/keys).

### Running

Just decorate your python functions with `@op` and run them within `LzyEnv` block! **Type annotations are required.**

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

You can also save execution results using whiteboards. Just declare a dataclass and pass it as a `whiteboard` argument to the `LzyEnv`:

```python
@dataclass
class GraphResult:
    dataset: DataSet = None
    model: CatBoostClassifier = None


if __name__ == '__main__':
    wb = GraphResult()
    with LzyEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem", whiteboard=wb):
        wb.dataset = dataset()
        wb.model = learn(wb.dataset)
        result = predict(wb.model, np.array([9, 1]))
        wb_id = wb.id()
    print(wb_id)
```

And then load them back!

```python
with LzyEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem") as env:
    wb = env.get_whiteboard(wb_id, GraphResult)
    print(wb.model)
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

## Deployment

### Requirements

1) terraform
2) azure subscription
3) azure cli

### Steps

1) create terraform module
2) import [azure_common](deployment/tf/modules/azure_common) module
3) az login
4) create GitHub OAuth App
5) generate client secret
6) create tvars.json file with the following content

```
{
  "github-client-id": "",
  "github-secret": ""
}
```

7) copy client ID and client secret from GitHub OAuth App and paste to the corresponding json fields 
8) terraform init
9) terraform plan -var-file=tvars.json -out=tfplan
10) terraform apply tfplan

### Example

In example, we use backend `azurerm` for remote terraform state storing

```
terraform {
  backend "azurerm" {
    resource_group_name  = "my-lzy-terraformstate"
    storage_account_name = "mylzytfstatestorage"
    container_name       = "terraformstate"
    key                  = "lzy.terraform.tfstate"
  }
}

module "azure_common" {
  source                     = "git@github.com:lambda-zy/lzy.git//deployment/tf/modules/azure_common"
  installation_name          = "my-lzy-installation"
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "master"
  server-image               = "lzydock/lzy-server:master"
}
```
