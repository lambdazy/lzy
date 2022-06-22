# noinspection PyPackageRequirements
from catboost import CatBoostClassifier, CatBoostRegressor, CatBoostRanker
from lzy.api.v1 import op, Provisioning
from lzy.api.v1.env import LzyRemoteEnv
from lzy.injections.extensions import extend


@extend((CatBoostClassifier, CatBoostRegressor, CatBoostRanker))
def fit(self, *args, provisioning: Provisioning = None, **kwargs):
    if provisioning:
        if provisioning.gpu:
            self._init_params['task_type'] = 'GPU'
            self._init_params['devices'] = '0:1'
        else:
            self._init_params['task_type'] = 'CPU'
            self._init_params['devices'] = None

        @op(gpu=provisioning.gpu)
        def train(model: CatBoostClassifier, x, *fit_args, **fit_kwargs) -> CatBoostClassifier:
            # noinspection PyProtectedMember
            x = x.__lzy_origin__  # use __lzy_origin as catboost checks the type of data
            model.fit(x, *fit_args, **fit_kwargs)
            return model

        with LzyRemoteEnv().workflow('catboost'):
            result = train(self, args[0], *(args[1:]), **kwargs)

        # update internal state is case of running `fit(...)` and not `model = fit(...)`
        # noinspection PyProtectedMember,PyUnresolvedReferences
        self._object = result._object
        self._set_trained_model_attributes()
        return self
    else:
        return self.original_fit(*args, **kwargs)
