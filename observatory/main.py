"""FastAPI application entry point."""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from observatory.config import config
from observatory.database import init_db, close_db
from observatory.poller.client import close_client
from observatory.poller.scheduler import setup_scheduler, run_initial_poll
from observatory.web.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    print("🔭 Starting Moltbook Observatory...")
    
    # Validate config
    config.validate()
    
    # Initialize database
    await init_db()
    
    # Set up and start scheduler (unless disabled)
    scheduler = None
    initial_poll_task = None
    
    if config.DISABLE_POLL:
        print("⏸️  Polling is disabled")
    else:
        scheduler = setup_scheduler()
        scheduler.start()
        print("📡 Background scheduler started")
        
        # Run initial data fetch in background (don't block startup)
        import asyncio
        initial_poll_task = asyncio.create_task(run_initial_poll())
        print("📊 Initial data fetch started in background")
    
    yield
    
    # Shutdown
    print("Shutting down...")
    if initial_poll_task and not initial_poll_task.done():
        initial_poll_task.cancel()
        try:
            await initial_poll_task
        except asyncio.CancelledError:
            pass
    if scheduler is not None:
        scheduler.shutdown()
    await close_client()
    await close_db()
    print("Goodbye! 🦞")


# Create FastAPI app
app = FastAPI(
    title="Moltbook Observatory",
    description="Passive monitoring and analytics dashboard for Moltbook",
    version="0.1.0",
    lifespan=lifespan,
)

# Mount static files
static_path = Path(__file__).parent / "static"
static_path.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Include routes
app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
