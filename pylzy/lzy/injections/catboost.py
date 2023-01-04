from dataclasses import dataclass
from typing import Union, Optional


def inject_catboost() -> None:
    from lzy.api.v1 import Provisioning, op, Lzy
    from lzy.api.v1.provisioning import GpuType
    from lzy.injections.extensions import extend

    # noinspection PyPackageRequirements
    from catboost import CatBoostClassifier, CatBoostRanker, CatBoostRegressor

    @dataclass
    class UnfitCatboostModel:
        model: Union[CatBoostClassifier, CatBoostRegressor, CatBoostRanker]

    @extend((CatBoostClassifier, CatBoostRegressor, CatBoostRanker))
    def fit(self, *args, cpu_type: Optional[str] = None, cpu_count: Optional[int] = None,
            gpu_type: Optional[str] = None, gpu_count: Optional[int] = None, ram_size_gb: Optional[int] = None,
            **kwargs):
        if cpu_count or cpu_type or gpu_type or gpu_count or ram_size_gb:
            provisioning = Provisioning.default().override(Provisioning(cpu_count=cpu_count, cpu_type=cpu_type,
                                                                        gpu_type=gpu_type, gpu_count=gpu_count,
                                                                        ram_size_gb=ram_size_gb))
            provisioning.validate()

            if provisioning.gpu_type is not None and provisioning.gpu_type != GpuType.NO_GPU.value:
                self._init_params["task_type"] = "GPU"
                self._init_params["devices"] = "0:1"
            else:
                self._init_params["task_type"] = "CPU"
                self._init_params["devices"] = None

            @op
            def train(holder: UnfitCatboostModel, x, *fit_args, **fit_kwargs) -> CatBoostClassifier:
                # noinspection PyUnresolvedReferences
                holder.model.original_fit(x, *fit_args, **fit_kwargs)
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
