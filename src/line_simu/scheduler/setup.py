from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from line_simu.config import settings

scheduler: AsyncIOScheduler | None = None


def init_scheduler() -> None:
    global scheduler
    jobstores = {"default": SQLAlchemyJobStore(url=settings.database_url_sync)}
    scheduler = AsyncIOScheduler(jobstores=jobstores, timezone="UTC")
    scheduler.start()

    from line_simu.scheduler.tasks import (
        check_inactive_sessions,
        check_registration_deliveries,
    )

    scheduler.add_job(
        check_inactive_sessions,
        "interval",
        minutes=10,
        id="check_inactive_sessions",
        replace_existing=True,
    )

    scheduler.add_job(
        check_registration_deliveries,
        "interval",
        hours=1,
        id="check_registration_deliveries",
        replace_existing=True,
    )


def shutdown_scheduler() -> None:
    if scheduler:
        scheduler.shutdown()


def get_scheduler() -> AsyncIOScheduler:
    assert scheduler is not None, "Scheduler not initialized"
    return scheduler
