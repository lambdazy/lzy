from asyncio import get_event_loop

import grpc.aio as aio

from lzy.api.v2.remote_grpc.workflow_service_client import WorkflowServiceClient


async def main():
    async with aio.insecure_channel("localhost:8080") as channel:
        client: WorkflowServiceClient = WorkflowServiceClient(channel=channel)
        wflow_id, endpoint = await client.create_workflow("test")
        print(wflow_id, endpoint.bucket, endpoint.creds)


def run():
    get_event_loop().run_until_complete(main())


if __name__ == "__main__":
    run()
