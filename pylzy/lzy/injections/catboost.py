from dataclasses import dataclass
from typing import Union, Optional


import lzy.api.v1.provisioning as lp
from lzy.api.v1 import op, Lzy
from lzy.injections.extensions import extend


def inject_catboost() -> None:
    # noinspection PyPackageRequirements
    from catboost import CatBoostClassifier, CatBoostRanker, CatBoostRegressor

    @dataclass
    class UnfitCatboostModel:
        model: Union[CatBoostClassifier, CatBoostRegressor, CatBoostRanker]

    @extend((CatBoostClassifier, CatBoostRegressor, CatBoostRanker))
    def fit(
        self,
        *args,
        provisioning: Optional[lp.Provisioning] = None,
        cpu_type: lp.StringRequirement = None,
        cpu_count: lp.IntegerRequirement = None,
        gpu_type: lp.StringRequirement = None,
        gpu_count: lp.IntegerRequirement = None,
        ram_size_gb: lp.IntegerRequirement = None,
        **kwargs
    ):

        if cpu_count or cpu_type or gpu_type or gpu_count or ram_size_gb:
            provisioning = provisioning or lp.Provisioning()
            provisioning = provisioning.override(
                cpu_count=cpu_count,
                cpu_type=cpu_type,
                gpu_type=gpu_type,
                gpu_count=gpu_count,
                ram_size_gb=ram_size_gb,
            )

            if provisioning.gpu_type is not None and provisioning.gpu_type != lp.GpuType.NO_GPU.value:
                self._init_params["task_type"] = "GPU"
                self._init_params["devices"] = "0:1"
            else:
                self._init_params["task_type"] = "CPU"
                self._init_params["devices"] = None

            @op(lazy_arguments=False)
            def train(holder: UnfitCatboostModel, x, *fit_args, **fit_kwargs) -> CatBoostClassifier:
                # noinspection PyUnresolvedReferences
                holder.model.fit(x, *fit_args, **fit_kwargs)
                return holder.model

            with Lzy().workflow("catboost", interactive=False, provisioning=provisioning):
                result = train(UnfitCatboostModel(self), args[0], *(args[1:]), **kwargs)

                # update internal state is case of running `fit(...)` and not `model = fit(...)`
                # noinspection PyProtectedMember,PyUnresolvedReferences
                self._object = result._object
                self._set_trained_model_attributes()
            return self
        else:
            return self.original_fit(*args, **kwargs)
