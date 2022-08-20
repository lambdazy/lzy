from asyncio import get_event_loop

from grpclib.client import Channel

from lzy.api.v2.remote_grpc.workflow_service_client import (
    StorageEndpoint,
    WorkflowServiceClient,
)

HOST = ""
PORT = 8080


async def main():
    async with Channel(HOST, PORT) as chnl:
        client: WorkflowServiceClient = WorkflowServiceClient(channel=chnl)
        async for (wflow_id, endpoint) in client.create_workflow("test"):
            print(wflow_id, endpoint.bucket, endpoint.creds)


def run():
    get_event_loop().run_until_complete(main())


if __name__ == "__main__":
    run()
