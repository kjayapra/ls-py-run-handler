from aiobotocore.session import get_session
from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

from ls_py_handler.api.routes.runs import router as runs_router
from ls_py_handler.config.settings import settings

app = FastAPI(
    title=settings.APP_TITLE,
    description=settings.APP_DESCRIPTION,
    version=settings.APP_VERSION,
)

# Include routers
app.include_router(runs_router)


@app.on_event("startup")
async def startup_event():
    """Initialize resources when the application starts."""
    # Initialize cache
    try:
        FastAPICache.init(RedisBackend(), prefix="ls-py-handler-cache")
        print(f"Initialized cache with Redis at {settings.REDIS_URL}")
    except Exception as e:
        print(f"Cache initialization failed: {e}, using in-memory fallback")
        from fastapi_cache.backends.inmemory import InMemoryBackend
        FastAPICache.init(InMemoryBackend(), prefix="ls-py-handler-cache")
    
    # Create S3 bucket if it doesn't exist
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
    ) as s3:
        try:
            await s3.create_bucket(Bucket=settings.S3_BUCKET_NAME)
            print(f"Created S3 bucket: {settings.S3_BUCKET_NAME}")
        except Exception:
            print("Tried to create S3 bucket, but it already exists. No action taken.")


@app.get("/")
async def root():
    """
    Root endpoint to verify the API is running.
    """
    return {"message": settings.APP_TITLE + " API"}
