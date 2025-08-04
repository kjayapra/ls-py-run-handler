import asyncio
import uuid
from typing import Any, Dict, List, Optional

import asyncpg
import orjson
from aiobotocore.session import get_session
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import UUID4, BaseModel, Field

from ls_py_handler.config.settings import settings

router = APIRouter(prefix="/runs", tags=["runs"])


def build_batch_with_streaming_json(runs: List["Run"]) -> tuple[bytes, List[Dict[str, Any]]]:
    """
    Build JSON batch with streaming approach for better memory efficiency.
    
    This approach builds JSON incrementally while tracking field positions,
    avoiding the need to search through the entire batch.
    """
    if not runs:
        return b"[]", []

    # Pre-calculate run data and field JSON
    run_data_list = []
    for run in runs:
        run_dict = run.model_dump()
        field_jsons = {}
        
        for field in ["inputs", "outputs", "metadata"]:
            field_value = run_dict.get(field, {})
            field_jsons[field] = orjson.dumps(field_value)
        
        run_data_list.append({
            "run_dict": run_dict,
            "field_jsons": field_jsons
        })

    # Build JSON batch incrementally with position tracking
    batch_parts = [b'[']
    current_position = 1  # Start after '['
    field_references = []

    for i, run_data in enumerate(run_data_list):
        if i > 0:
            batch_parts.append(b',')
            current_position += 1

        # Serialize the entire run
        run_json = orjson.dumps(run_data["run_dict"])
        
        # Calculate field positions within this run
        field_refs = {}
        run_start_pos = current_position
        
        for field in ["inputs", "outputs", "metadata"]:
            field_json = run_data["field_jsons"][field]
            
            # Find field position within the run JSON
            field_pos_in_run = run_json.find(field_json)
            
            if field_pos_in_run != -1:
                field_start = run_start_pos + field_pos_in_run
                field_end = field_start + len(field_json)
                field_refs[field] = f"s3://{settings.S3_BUCKET_NAME}/{{object_key}}#{field_start}:{field_end}/{field}"
            else:
                field_refs[field] = ""
        
        field_references.append(field_refs)
        
        # Add run JSON to batch
        batch_parts.append(run_json)
        current_position += len(run_json)

    batch_parts.append(b']')
    
    # Combine all parts
    batch_data = b''.join(batch_parts)
    
    return batch_data, field_references


async def upload_batch_to_s3(s3: Any, batch_data: bytes, object_key: str) -> None:
    """Upload batch data to S3 asynchronously."""
    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=batch_data,
        ContentType="application/json",
    )


async def prepare_database_insert_data(runs: List["Run"], field_references_list: List[Dict[str, str]], object_key: str) -> List[tuple]:
    """Prepare database insert data asynchronously."""
    insert_data = []
    
    for i, run in enumerate(runs):
        # Get pre-calculated field references and substitute object_key
        field_refs = field_references_list[i].copy()  # Copy to avoid mutating original
        for field_name in field_refs:
            if field_refs[field_name]:
                field_refs[field_name] = field_refs[field_name].format(object_key=object_key)
        
        insert_data.append((
            run.id,
            run.trace_id,
            run.name,
            field_refs["inputs"],
            field_refs["outputs"],
            field_refs["metadata"],
        ))
    
    return insert_data


async def fetch_all_fields_optimized(s3: Any, field_refs: Dict[str, str]) -> Dict[str, Any]:
    """
    Fetch all fields with minimal S3 requests by consolidating byte ranges.
    Reduces 3 S3 calls to 1 call per S3 object.
    """
    # Group references by S3 object
    s3_objects = {}

    for field_name, ref in field_refs.items():
        if ref and ref.startswith("s3://"):
            bucket, key, offsets, field = parse_s3_ref(ref)
            if bucket and key and offsets:
                if key not in s3_objects:
                    s3_objects[key] = {
                        "bucket": bucket,
                        "fields": {},
                        "min_start": float("inf"),
                        "max_end": 0,
                    }

                start_offset, end_offset = offsets
                s3_objects[key]["fields"][field_name] = {
                    "start": start_offset,
                    "end": end_offset,
                    "field": field,
                }
                s3_objects[key]["min_start"] = min(
                    s3_objects[key]["min_start"], start_offset
                )
                s3_objects[key]["max_end"] = max(
                    s3_objects[key]["max_end"], end_offset
                )

    results = {"inputs": {}, "outputs": {}, "metadata": {}}

    # Fetch each S3 object with consolidated range
    for key, obj_info in s3_objects.items():
        try:
            # Use consolidated byte range
            start = obj_info["min_start"]
            end = obj_info["max_end"]
            byte_range = f"bytes={start}-{end-1}"

            response = await s3.get_object(
                Bucket=obj_info["bucket"], Key=key, Range=byte_range
            )
            async with response["Body"] as stream:
                consolidated_data = await stream.read()

            # Extract individual fields from consolidated data
            for field_name, field_info in obj_info["fields"].items():
                field_start = field_info["start"] - start
                field_end = field_info["end"] - start
                field_data = consolidated_data[field_start:field_end]

                try:
                    results[field_name] = orjson.loads(field_data)
                except Exception as parse_error:
                    print(f"Error parsing {field_name}: {parse_error}")
                    results[field_name] = {}

        except Exception as e:
            print(f"Error fetching S3 object {key}: {e}")
            # Fallback to empty results for this object
            for field_name in obj_info["fields"]:
                results[field_name] = {}

    return results


def parse_s3_ref(ref):
    """Parse S3 reference string into components."""
    if not ref or not ref.startswith("s3://"):
        return None, None, None, None

    parts = ref.split("/")
    bucket = parts[2]
    key = "/".join(parts[3:]).split("#")[0]

    if "#" in ref:
        offset_part = ref.split("#")[1]
        if ":" in offset_part and "/" in offset_part:
            offsets, field = offset_part.split("/")
            start_offset, end_offset = map(int, offsets.split(":"))
            return bucket, key, (start_offset, end_offset), field

    return bucket, key, None, None


class Run(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid.uuid4)
    trace_id: UUID4
    name: str
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}


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
    Create new runs in batch.

    Takes a JSON array of Run objects, uploads them to MinIO,
    and stores references to certain fields in PostgreSQL.
    """
    if not runs:
        raise HTTPException(status_code=400, detail="No runs provided")

    # Prepare the batch for S3 upload using streaming approach
    batch_id = str(uuid.uuid4())
    
    batch_data, field_references_list = build_batch_with_streaming_json(runs)

    object_key = f"batches/{batch_id}.json"

    # Execute S3 upload and database preparation in parallel
    upload_task = upload_batch_to_s3(s3, batch_data, object_key)
    db_prep_task = prepare_database_insert_data(runs, field_references_list, object_key)
    
    # Wait for both operations to complete
    _, insert_data = await asyncio.gather(upload_task, db_prep_task)

    # Single batch insert operation using executemany
    await db.executemany(
        """
        INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        insert_data
    )
    
    # Get the inserted IDs (they're the same as the original run IDs)
    inserted_ids = [str(run.id) for run in runs]

    return {"status": "created", "run_ids": inserted_ids}


@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(
    run_id: UUID4,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Get a run by its ID.
    """
    # Fetch the run from the PG
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
