"""Run the first version on one task queue; split workers as deployment requires."""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from .config_activities import ACTIVITIES as CONFIG_ACTIVITIES
from .fabric_activities import ACTIVITIES as FABRIC_ACTIVITIES
from .rebuild_activities import ACTIVITIES as REBUILD_ACTIVITIES
from .source_activities import ACTIVITIES as SOURCE_ACTIVITIES
from .workflows import WORKFLOWS


TASK_QUEUE = os.getenv("MIGRATION_TASK_QUEUE", "migration-workflows")


async def main() -> None:
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
                                  namespace=os.getenv("TEMPORAL_NAMESPACE", "default"))
    with ThreadPoolExecutor(max_workers=16) as executor:
        worker = Worker(client, task_queue=TASK_QUEUE, workflows=WORKFLOWS,
                        activities=[*CONFIG_ACTIVITIES, *SOURCE_ACTIVITIES,
                                    *FABRIC_ACTIVITIES, *REBUILD_ACTIVITIES],
                        activity_executor=executor)
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
