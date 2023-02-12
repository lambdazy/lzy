from lzy.api.v1 import op, Lzy

# Catboost requires both pandas & numpy
# noinspection PyPackageRequirements
import pandas as pd
# noinspection PyPackageRequirements
import numpy as np


@op
def accept_return_df(frame: pd.DataFrame) -> pd.DataFrame:
    print(f"size_input: {len(frame)}")
    return frame


with Lzy().workflow(name="wf", interactive=False):
    df = pd.DataFrame(np.random.choice(['lzy', 'yzl', 'zly'], size=(5000000, 3)))
    # noinspection PyNoneFunctionAssignment
    out = accept_return_df(df)
    print(f"size_output: {len(out)}")
