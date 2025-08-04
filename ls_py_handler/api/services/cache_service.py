"""
Cache management service for the LS Run Handler.
"""
from typing import Optional
from fastapi_cache import FastAPICache
from pydantic import UUID4


class CacheService:
    """Service for managing cache operations."""
    
    @staticmethod
    async def invalidate_run_cache(run_id: UUID4) -> None:
        """Invalidate cache for a specific run."""
        cache_key = f"get_run:{run_id}"
        try:
            await FastAPICache.clear(cache_key)
        except Exception:
            # Cache invalidation failures shouldn't break the request
            pass
    
    @staticmethod
    async def invalidate_search_cache(
        trace_id: Optional[UUID4] = None,
        name: Optional[str] = None
    ) -> None:
        """Invalidate search cache for specific parameters."""
        # For search results, we could invalidate all search cache
        # or be more specific based on the trace_id/name patterns
        try:
            # Clear all search cache (simple approach)
            await FastAPICache.clear(pattern="search_runs:*")
        except Exception:
            # Cache invalidation failures shouldn't break the request
            pass
    
    @staticmethod
    async def clear_all_cache() -> None:
        """Clear all application cache (use with caution)."""
        try:
            await FastAPICache.clear()
        except Exception:
            pass