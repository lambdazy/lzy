from lzy.api.v2 import op, Lzy, GrpcRuntime


@op
def just_return_none() -> None:
    return None

runtime = GrpcRuntime(
    username="artolord",
    address="158.160.44.118:8122",
    key_path="/Users/artolord/.ssh/private.pem"
)

with Lzy(runtime=runtime).workflow(name="artolord:wf", interactive=False):
    # noinspection PyNoneFunctionAssignment
    res = just_return_none()
    print(res)
