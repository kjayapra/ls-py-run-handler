"""
Cache performance benchmarks - comparing cache miss vs cache hit.
"""
import asyncio
import uuid
import pytest
from httpx import AsyncClient

from ls_py_handler.main import app
from ls_py_handler.api.models.run import Run


@pytest.fixture(scope="session", autouse=True)
def initialize_cache():
    """Initialize cache for all benchmark tests."""
    from fastapi_cache import FastAPICache
    try:
        from fastapi_cache.backends.redis import RedisBackend
        FastAPICache.init(RedisBackend(), prefix="ls-py-handler-cache")
        print("Initialized cache with Redis backend")
    except Exception as e:
        print(f"Redis failed: {e}, using in-memory cache")
        from fastapi_cache.backends.inmemory import InMemoryBackend
        FastAPICache.init(InMemoryBackend(), prefix="ls-py-handler-cache")
    yield


def create_test_run_with_large_data(field_size_kb: int = 100) -> Run:
    """Create a test run with large data fields."""
    large_data = "x" * (field_size_kb * 1024)  # Create large string
    
    return Run(
        trace_id=uuid.uuid4(),
        name=f"Cache Test Run {uuid.uuid4().hex[:8]}",
        inputs={"large_field": large_data},
        outputs={"large_result": large_data},
        metadata={"large_metadata": large_data},
    )


@pytest.fixture
def test_run_id():
    """Create a test run and return its ID for cache testing."""
    async def create_run():
        test_run = create_test_run_with_large_data(field_size_kb=100)
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Create the run
            run_dict = test_run.model_dump()
            run_dict["id"] = str(run_dict["id"])
            run_dict["trace_id"] = str(run_dict["trace_id"])
            
            response = await client.post("/runs", json=[run_dict])
            assert response.status_code == 201
            
            run_id = response.json()["run_ids"][0]
            return run_id
    
    return asyncio.run(create_run())


def test_get_run_cache_miss(benchmark, test_run_id):
    """Benchmark GET request on cache miss (first request)."""
    
    def sync_wrapper():
        async def get_run_first_time():
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(f"/runs/{test_run_id}")
                assert response.status_code == 200
                return response.json()
        return asyncio.run(get_run_first_time())
    
    result = benchmark(sync_wrapper)
    assert "id" in result
    assert result["id"] == test_run_id


def test_get_run_cache_hit(benchmark, test_run_id):
    """Benchmark GET request on cache hit (subsequent requests)."""
    
    # First, make a request to populate the cache
    async def populate_cache():
        async with AsyncClient(app=app, base_url="http://test") as client:
            initial_response = await client.get(f"/runs/{test_run_id}")
            assert initial_response.status_code == 200
    
    asyncio.run(populate_cache())
    
    # Now benchmark the cached request
    def sync_wrapper():
        async def get_run_from_cache():
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(f"/runs/{test_run_id}")
                assert response.status_code == 200
                return response.json()
        return asyncio.run(get_run_from_cache())
    
    result = benchmark(sync_wrapper)
    assert "id" in result
    assert result["id"] == test_run_id


def test_search_runs_cache_miss(benchmark):
    """Benchmark search request on cache miss."""
    
    def sync_wrapper():
        # Create test runs with unique identifiers
        async def setup_and_search():
            test_runs = []
            unique_name = f"CacheMissTest_{uuid.uuid4().hex[:8]}"
            
            for i in range(3):
                run = create_test_run_with_large_data(field_size_kb=50)
                run.name = f"{unique_name}_Run_{i}"
                test_runs.append(run)
            
            # Create the runs
            async with AsyncClient(app=app, base_url="http://test") as client:
                run_dicts = []
                for run in test_runs:
                    run_dict = run.model_dump()
                    run_dict["id"] = str(run_dict["id"])
                    run_dict["trace_id"] = str(run_dict["trace_id"])
                    run_dicts.append(run_dict)
                
                create_response = await client.post("/runs", json=run_dicts)
                assert create_response.status_code == 201
                
                # Perform the search (cache miss)
                response = await client.get(f"/runs/search?name={unique_name}")
                assert response.status_code == 200
                return response.json()
        return asyncio.run(setup_and_search())
    
    result = benchmark(sync_wrapper)
    assert result["count"] == 3


def test_search_runs_cache_hit(benchmark):
    """Benchmark search request on cache hit."""
    
    # Setup data and populate cache
    unique_name = f"CacheHitTest_{uuid.uuid4().hex[:8]}"
    
    async def setup_data():
        test_runs = []
        for i in range(3):
            run = create_test_run_with_large_data(field_size_kb=50)
            run.name = f"{unique_name}_Run_{i}"
            test_runs.append(run)
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            run_dicts = []
            for run in test_runs:
                run_dict = run.model_dump()
                run_dict["id"] = str(run_dict["id"])
                run_dict["trace_id"] = str(run_dict["trace_id"])
                run_dicts.append(run_dict)
            
            create_response = await client.post("/runs", json=run_dicts)
            assert create_response.status_code == 201
            
            # Make initial request to populate cache
            initial_search = await client.get(f"/runs/search?name={unique_name}")
            assert initial_search.status_code == 200
    
    asyncio.run(setup_data())
    
    # Now benchmark the cached search
    def sync_wrapper():
        async def search_from_cache():
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(f"/runs/search?name={unique_name}")
                assert response.status_code == 200
                return response.json()
        return asyncio.run(search_from_cache())
    
    result = benchmark(sync_wrapper)
    assert result["count"] == 3


def test_multiple_cache_hits(benchmark, test_run_id):
    """Benchmark multiple consecutive cached requests."""
    
    # First request to populate cache
    async def populate_cache():
        async with AsyncClient(app=app, base_url="http://test") as client:
            initial_response = await client.get(f"/runs/{test_run_id}")
            assert initial_response.status_code == 200
    
    asyncio.run(populate_cache())
    
    # Benchmark 5 consecutive cached requests
    def sync_wrapper():
        async def multiple_cached_requests():
            async with AsyncClient(app=app, base_url="http://test") as client:
                results = []
                for _ in range(5):
                    response = await client.get(f"/runs/{test_run_id}")
                    assert response.status_code == 200
                    results.append(response.json())
                return results
        return asyncio.run(multiple_cached_requests())
    
    results = benchmark(sync_wrapper)
    assert len(results) == 5
    assert all(result["id"] == test_run_id for result in results)