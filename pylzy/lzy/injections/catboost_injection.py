# noinspection PyPackageRequirements
from catboost import CatBoostClassifier, CatBoostRanker, CatBoostRegressor

from lzy.api.v2 import Provisioning, op, Lzy
from lzy.api.v2.provisioning import GpuType
from lzy.injections.extensions import extend


@extend((CatBoostClassifier, CatBoostRegressor, CatBoostRanker))
def fit(self, *args, provisioning: Provisioning = Provisioning.default(), **kwargs):
    if provisioning:
        if provisioning.gpu_type is not None and provisioning.gpu_type != GpuType.NO_GPU.value:
            self._init_params["task_type"] = "GPU"
            self._init_params["devices"] = "0:1"
        else:
            self._init_params["task_type"] = "CPU"
            self._init_params["devices"] = None

        @op
        def train(
            model: CatBoostClassifier, x, *fit_args, **fit_kwargs
        ) -> CatBoostClassifier:
            model.original_fit(x, *fit_args, **fit_kwargs)
            return model

        with Lzy().workflow("catboost", interactive=False, provisioning=provisioning):
            result = train(self, args[0], *(args[1:]), **kwargs)

            # update internal state is case of running `fit(...)` and not `model = fit(...)`
            # noinspection PyProtectedMember,PyUnresolvedReferences
            self._object = result._object
            self._set_trained_model_attributes()
        return self
    else:
        return self.original_fit(*args, **kwargs)
