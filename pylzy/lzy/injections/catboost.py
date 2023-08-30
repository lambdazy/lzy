from dataclasses import dataclass
from typing import Union, Optional

from lzy.env.base import is_specified
from lzy.env.provisioning.provisioning import Provisioning, NO_GPU
from lzy.api.v1 import op, Lzy
from lzy.injections.extensions import extend


__all__ = ['inject_catboost']


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
        provisioning: Optional[Provisioning] = None,
        **kwargs
    ):
        if provisioning:
            if (
                provisioning.gpu_type is not None and
                is_specified(provisioning.gpu_type) and
                provisioning.gpu_type != NO_GPU
            ):
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

            with Lzy().workflow("catboost", interactive=False).with_provisioning(provisioning):
                result = train(UnfitCatboostModel(self), args[0], *(args[1:]), **kwargs)

                # update internal state is case of running `fit(...)` and not `model = fit(...)`
                # noinspection PyProtectedMember,PyUnresolvedReferences
                self._object = result._object
                self._set_trained_model_attributes()
            return self

        return self.original_fit(*args, **kwargs)
