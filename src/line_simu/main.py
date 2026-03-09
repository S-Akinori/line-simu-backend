from contextlib import asynccontextmanager

from fastapi import FastAPI

from line_simu.api import health, webhook
from line_simu.db.connection import close_db_pool, init_db_pool
from line_simu.scheduler.setup import init_scheduler, shutdown_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    init_scheduler()
    yield
    shutdown_scheduler()
    await close_db_pool()


app = FastAPI(title="LINE Compensation Simulator", lifespan=lifespan)
app.include_router(webhook.router)
app.include_router(health.router)
