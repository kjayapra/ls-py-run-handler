"""
Clean, refactored runs API endpoints.
"""
import asyncio
import uuid
from typing import Any, List, Optional

import asyncpg
from aiobotocore.session import get_session
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import UUID4

from ls_py_handler.api.models.run import Run
from ls_py_handler.api.services.batch_service import (
    build_batch_with_streaming_json,
    upload_batch_to_s3,
    prepare_database_insert_data,
)
from ls_py_handler.api.services.s3_service import (
    fetch_all_fields_optimized,
    fetch_multiple_runs_optimized,
)
from ls_py_handler.config.settings import settings

router = APIRouter(prefix="/runs", tags=["runs"])


async def get_db_conn():
    """Get a database connection."""
    conn = await asyncpg.connect(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
    )
    try:
        yield conn
    finally:
        await conn.close()


async def get_s3_client():
    """Get an S3 client for MinIO."""
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
    ) as client:
        yield client


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_runs(
    runs: List[Run],
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Create new runs in batch with optimized processing.
    
    Optimizations:
    - Streaming JSON generation (O(n) instead of O(n²))
    - Parallel S3 upload and DB preparation
    - Batch database insert
    """
    if not runs:
        raise HTTPException(status_code=400, detail="No runs provided")

    # Generate batch data with streaming approach
    batch_id = str(uuid.uuid4())
    batch_data, field_references_list = build_batch_with_streaming_json(runs)
    object_key = f"batches/{batch_id}.json"

    # Execute S3 upload and database preparation in parallel
    upload_task = upload_batch_to_s3(s3, batch_data, object_key)
    db_prep_task = prepare_database_insert_data(runs, field_references_list, object_key)
    
    # Wait for both operations to complete
    _, insert_data = await asyncio.gather(upload_task, db_prep_task)

    # Single batch insert operation
    await db.executemany(
        """
        INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        insert_data
    )
    
    inserted_ids = [str(run.id) for run in runs]
    return {"status": "created", "run_ids": inserted_ids}


@router.get("/search", status_code=status.HTTP_200_OK)
async def search_runs(
    trace_id: Optional[UUID4] = None,
    name: Optional[str] = None,
    limit: int = 50,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Search runs by trace_id or name with optimized multi-run S3 retrieval.
    
    Optimizations:
    - Flexible search parameters (trace_id and/or name)
    - Multi-run S3 field consolidation
    - Efficient database queries with indexes
    
    Parameters:
    - trace_id: Filter by trace ID (optional)
    - name: Filter by name (optional, supports partial matching)
    - limit: Maximum number of results (default: 50)
    """
    if not trace_id and not name:
        raise HTTPException(
            status_code=400, 
            detail="At least one search parameter (trace_id or name) must be provided"
        )

    # Build dynamic query based on provided parameters
    conditions = []
    params = []
    param_counter = 1

    if trace_id:
        conditions.append(f"trace_id = ${param_counter}")
        params.append(trace_id)
        param_counter += 1

    if name:
        conditions.append(f"name ILIKE ${param_counter}")
        params.append(f"%{name}%")
        param_counter += 1

    where_clause = " AND ".join(conditions)
    params.append(limit)

    query = f"""
        SELECT id, trace_id, name, inputs, outputs, metadata
        FROM runs
        WHERE {where_clause}
        ORDER BY id DESC
        LIMIT ${param_counter}
    """

    # Execute database query
    rows = await db.fetch(query, *params)
    
    if not rows:
        return {"runs": [], "count": 0}

    # Convert rows to list of dicts
    runs_data = [dict(row) for row in rows]

    # Use optimized multi-run S3 fetching
    results = await fetch_multiple_runs_optimized(s3, runs_data)

    return {
        "runs": results,
        "count": len(results)
    }


@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(
    run_id: UUID4,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Get a run by its ID with optimized S3 field retrieval.
    
    Optimizations:
    - Consolidated S3 requests (3 → 1 per run)
    - Byte-range requests for efficient data transfer
    """
    # Fetch run metadata from database
    row = await db.fetchrow(
        """
        SELECT id, trace_id, name, inputs, outputs, metadata
        FROM runs
        WHERE id = $1
        """,
        run_id,
    )

    if not row:
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")

    run_data = dict(row)

    # Use optimized field fetching with consolidated S3 requests
    field_refs = {
        "inputs": run_data["inputs"],
        "outputs": run_data["outputs"], 
        "metadata": run_data["metadata"]
    }
    
    field_data = await fetch_all_fields_optimized(s3, field_refs)

    return {
        "id": str(run_data["id"]),
        "trace_id": str(run_data["trace_id"]),
        "name": run_data["name"],
        "inputs": field_data["inputs"],
        "outputs": field_data["outputs"],
        "metadata": field_data["metadata"],
    }