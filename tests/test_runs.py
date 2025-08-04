import uuid
import pytest
import pytest_asyncio
from httpx import AsyncClient
from typing import List, Dict, Any

from ls_py_handler.main import app
from ls_py_handler.api.models.run import Run


def create_test_run(
    trace_id: uuid.UUID = None,
    name: str = "Test Run",
    inputs: Dict[str, Any] = None,
    outputs: Dict[str, Any] = None,
    metadata: Dict[str, Any] = None,
) -> Run:
    """Helper function to create a test run with default values."""
    return Run(
        trace_id=trace_id or uuid.uuid4(),
        name=name,
        inputs=inputs or {"default": "input"},
        outputs=outputs or {"default": "output"},
        metadata=metadata or {"default": "metadata"},
    )


def runs_to_dict_list(runs: List[Run]) -> List[Dict[str, Any]]:
    """Convert list of Run objects to dictionaries with string UUIDs."""
    run_dicts = []
    for run in runs:
        run_dict = run.model_dump()
        run_dict["id"] = str(run_dict["id"])
        run_dict["trace_id"] = str(run_dict["trace_id"])
        run_dicts.append(run_dict)
    return run_dicts


async def create_runs_via_api(client: AsyncClient, runs: List[Run]) -> Dict[str, Any]:
    """Helper function to create runs via POST API."""
    run_dicts = runs_to_dict_list(runs)
    response = await client.post("/runs", json=run_dicts)
    assert response.status_code == 201
    return response.json()


async def search_runs_via_api(
    client: AsyncClient,
    trace_id: uuid.UUID = None,
    name: str = None,
    limit: int = None,
) -> Dict[str, Any]:
    """Helper function to search runs via GET API."""
    params = []
    if trace_id:
        params.append(f"trace_id={trace_id}")
    if name:
        params.append(f"name={name}")
    if limit:
        params.append(f"limit={limit}")
    
    query_string = "&".join(params)
    url = f"/runs/search?{query_string}" if query_string else "/runs/search"
    
    response = await client.get(url)
    return response


def assert_search_result_structure(result: Dict[str, Any]):
    """Assert that search result has correct structure."""
    assert "runs" in result
    assert "count" in result
    assert isinstance(result["runs"], list)
    assert isinstance(result["count"], int)
    assert result["count"] == len(result["runs"])


def assert_run_data_integrity(run_data: Dict[str, Any]):
    """Assert that run data has all required fields."""
    required_fields = ["id", "trace_id", "name", "inputs", "outputs", "metadata"]
    for field in required_fields:
        assert field in run_data, f"Missing field: {field}"


@pytest_asyncio.fixture
async def test_client():
    """Fixture providing test client."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture
def sample_trace_ids():
    """Fixture providing sample trace IDs for tests."""
    return {
        "trace_1": uuid.uuid4(),
        "trace_2": uuid.uuid4(),
        "trace_3": uuid.uuid4(),
    }


@pytest.fixture
def sample_runs(sample_trace_ids):
    """Fixture providing sample runs for testing."""
    return [
        create_test_run(
            trace_id=sample_trace_ids["trace_1"],
            name="Machine Learning Model Training",
            inputs={"algorithm": "neural_network"},
            outputs={"accuracy": 0.95},
            metadata={"category": "ml"},
        ),
        create_test_run(
            trace_id=sample_trace_ids["trace_2"],
            name="Data Processing Pipeline",
            inputs={"data_source": "csv"},
            outputs={"processed_rows": 1000},
            metadata={"category": "etl"},
        ),
        create_test_run(
            trace_id=sample_trace_ids["trace_1"],  # Same trace as first run
            name="Machine Learning Feature Engineering",
            inputs={"features": ["age", "income"]},
            outputs={"feature_count": 15},
            metadata={"category": "ml"},
        ),
    ]


@pytest.mark.asyncio
async def test_create_and_get_run():
    """
    Test the POST /runs endpoint to create multiple runs
    and the GET /runs/{run_id} endpoint to retrieve them.
    """
    # Create test data for multiple runs
    run1 = Run(
        trace_id=uuid.uuid4(),
        name="Test Run 1",
        inputs={"prompt": "What is the capital of France?"},
        outputs={"answer": "Paris"},
        metadata={"model": "gpt-4", "temperature": 0.7},
    )

    run2 = Run(
        trace_id=uuid.uuid4(),
        name="Test Run 2",
        inputs={"prompt": "Tell me about machine learning"},
        outputs={"answer": "Machine learning is a branch of AI..."},
        metadata={"model": "gpt-3.5-turbo", "temperature": 0.5},
    )

    run3 = Run(
        trace_id=uuid.uuid4(),
        name="Test Run 3",
        inputs={"prompt": "Python code example"},
        outputs={"code": "print('Hello, World!')"},
        metadata={"model": "codex", "temperature": 0.2},
    )

    # Create a list of runs to send in a batch
    runs = [run1, run2, run3]

    # Convert Run objects to dictionaries with string UUIDs
    run_dicts = []
    for run in runs:
        run_dict = run.model_dump()
        # Convert UUID objects to strings
        run_dict["id"] = str(run_dict["id"])
        run_dict["trace_id"] = str(run_dict["trace_id"])
        run_dicts.append(run_dict)

    async with AsyncClient(app=app, base_url="http://test") as client:
        # Create the runs
        response = await client.post("/runs", json=run_dicts)

        # Check response status and structure
        assert response.status_code == 201
        assert "status" in response.json()
        assert response.json()["status"] == "created"
        assert "run_ids" in response.json()

        # Get the returned run IDs
        run_ids = response.json()["run_ids"]
        assert len(run_ids) == 3

        # Verify we can retrieve each run individually
        for i, run_id in enumerate(run_ids):
            get_response = await client.get(f"/runs/{run_id}")

            # Check response status
            assert get_response.status_code == 200

            # Verify the run data matches what we sent
            run_data = get_response.json()
            assert run_data["id"] == run_id

            # Verify run name matches the original
            expected_name = runs[i].name
            assert run_data["name"] == expected_name

            # Verify inputs, outputs, and metadata match
            assert run_data["inputs"] == runs[i].inputs
            assert run_data["outputs"] == runs[i].outputs
            assert run_data["metadata"] == runs[i].metadata


@pytest.mark.asyncio
async def test_search_runs_by_trace_id(test_client, sample_trace_ids, sample_runs):
    """Test searching runs by trace_id."""
    # Create the test runs
    await create_runs_via_api(test_client, sample_runs)
    
    # Search by trace_1 (should return 2 runs)
    response = await search_runs_via_api(test_client, trace_id=sample_trace_ids["trace_1"])
    assert response.status_code == 200
    
    result = response.json()
    assert_search_result_structure(result)
    assert result["count"] == 2
    
    # Verify correct runs returned
    returned_names = {run["name"] for run in result["runs"]}
    assert "Machine Learning Model Training" in returned_names
    assert "Machine Learning Feature Engineering" in returned_names
    assert "Data Processing Pipeline" not in returned_names
    
    # Verify data integrity
    for run_data in result["runs"]:
        assert_run_data_integrity(run_data)
        assert run_data["trace_id"] == str(sample_trace_ids["trace_1"])


@pytest.mark.asyncio
async def test_search_runs_by_name(test_client):
    """Test searching runs by name with partial matching."""
    # Create test runs with unique names using UUID for this specific test run
    test_uuid = uuid.uuid4().hex[:8]
    unique_runs = [
        create_test_run(
            name=f"SearchByName_{test_uuid}_ML_Model_Training",
            inputs={"algorithm": "neural_network"},
            outputs={"accuracy": 0.95},
        ),
        create_test_run(
            name=f"SearchByName_{test_uuid}_ML_Feature_Engineering", 
            inputs={"features": ["age", "income"]},
            outputs={"feature_count": 15},
        ),
        create_test_run(
            name=f"SearchByName_{test_uuid}_Data_Processing_Pipeline",
            inputs={"data_source": "csv"},
            outputs={"processed_rows": 1000},
        ),
    ]
    
    await create_runs_via_api(test_client, unique_runs)
    
    # Search by partial name with the unique test UUID
    search_pattern = f"SearchByName_{test_uuid}_ML"
    response = await search_runs_via_api(test_client, name=search_pattern)
    assert response.status_code == 200
    
    result = response.json()
    assert_search_result_structure(result)
    assert result["count"] == 2
    
    returned_names = {run["name"] for run in result["runs"]}
    assert f"SearchByName_{test_uuid}_ML_Model_Training" in returned_names
    assert f"SearchByName_{test_uuid}_ML_Feature_Engineering" in returned_names
    assert f"SearchByName_{test_uuid}_Data_Processing_Pipeline" not in returned_names
    
    # Test case-insensitive search
    response_lower = await search_runs_via_api(test_client, name=search_pattern.lower())
    assert response_lower.status_code == 200
    assert response_lower.json()["count"] == 2
    
    # Test partial word search for processing
    data_search_pattern = f"SearchByName_{test_uuid}_Data"
    response_partial = await search_runs_via_api(test_client, name=data_search_pattern)
    assert response_partial.status_code == 200
    result_partial = response_partial.json()
    assert result_partial["count"] == 1
    assert result_partial["runs"][0]["name"] == f"SearchByName_{test_uuid}_Data_Processing_Pipeline"


@pytest.mark.asyncio 
async def test_search_runs_by_both_trace_id_and_name(test_client):
    """Test searching runs with both trace_id and name parameters."""
    # Create unique trace IDs for this test
    unique_trace_1 = uuid.uuid4()
    unique_trace_2 = uuid.uuid4()
    unique_prefix = f"CombinedTest_{uuid.uuid4().hex[:8]}"
    
    # Create specific test runs for combined search
    runs = [
        create_test_run(
            trace_id=unique_trace_1,
            name=f"{unique_prefix}_Test_Analysis_Run",
            inputs={"data": "test_data"},
        ),
        create_test_run(
            trace_id=unique_trace_1,
            name=f"{unique_prefix}_Production_Analysis_Run",
            inputs={"data": "prod_data"},
        ),
        create_test_run(
            trace_id=unique_trace_2,
            name=f"{unique_prefix}_Test_Analysis_Run",  # Same name, different trace
            inputs={"data": "different_data"},
        ),
    ]
    
    await create_runs_via_api(test_client, runs)
    
    # Search by both trace_id and name (should return only first run)
    response = await search_runs_via_api(
        test_client, 
        trace_id=unique_trace_1, 
        name=f"{unique_prefix}_Test_Analysis"
    )
    assert response.status_code == 200
    
    result = response.json()
    assert_search_result_structure(result)
    assert result["count"] == 1
    
    returned_run = result["runs"][0]
    assert returned_run["name"] == f"{unique_prefix}_Test_Analysis_Run"
    assert returned_run["trace_id"] == str(unique_trace_1)


@pytest.mark.asyncio
async def test_search_runs_with_limit(test_client):
    """Test searching runs with limit parameter."""
    # Create multiple runs with unique names for this test
    unique_prefix = f"LimitTest_{uuid.uuid4().hex[:8]}"
    runs = [
        create_test_run(name=f"{unique_prefix}_Batch_Job_{i}", inputs={"batch_id": i})
        for i in range(5)
    ]
    
    await create_runs_via_api(test_client, runs)
    
    # Search with limit=3
    response = await search_runs_via_api(test_client, name=f"{unique_prefix}_Batch_Job", limit=3)
    assert response.status_code == 200
    
    result = response.json()
    assert_search_result_structure(result)
    assert result["count"] == 3
    
    # Search with limit=10 (should return all 5)
    response_all = await search_runs_via_api(test_client, name=f"{unique_prefix}_Batch_Job", limit=10)
    assert response_all.status_code == 200
    assert response_all.json()["count"] == 5


@pytest.mark.asyncio
async def test_search_runs_no_parameters(test_client):
    """Test search endpoint validation with no parameters."""
    response = await search_runs_via_api(test_client)
    assert response.status_code == 400
    assert "At least one search parameter" in response.json()["detail"]


@pytest.mark.asyncio
async def test_search_runs_no_results(test_client):
    """Test search endpoint with no matching results."""
    # Use a very unique search term that won't match anything
    unique_search = f"NonExistentRun_{uuid.uuid4().hex}"
    response = await search_runs_via_api(test_client, name=unique_search)
    assert response.status_code == 200
    
    result = response.json()
    assert_search_result_structure(result)
    assert result["count"] == 0
    assert result["runs"] == []
