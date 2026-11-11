import socket
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.worker import Worker


class WorkerRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def register(self, worker_id: str) -> Worker:
        worker = await self.session.get(Worker, worker_id)
        if worker is None:
            worker = Worker(id=worker_id, hostname=socket.gethostname(), status="online")
            self.session.add(worker)
        else:
            worker.hostname = socket.gethostname()
            worker.status = "online"
            worker.last_heartbeat = func.now()
        await self.session.flush()
        return worker

    async def heartbeat(self, worker_id: str) -> bool:
        result = await self.session.execute(
            update(Worker).where(Worker.id == worker_id).values(last_heartbeat=func.now(), status="online")
        )
        return result.rowcount == 1

    async def set_status(self, worker_id: str, status: str) -> None:
        await self.session.execute(update(Worker).where(Worker.id == worker_id).values(status=status))

    async def get(self, worker_id: str) -> Worker | None:
        return await self.session.get(Worker, worker_id)
