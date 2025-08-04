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

    # Upload the batch data
    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=batch_data,
        ContentType="application/json",
    )

    # Store references in PG using pre-calculated field references
    inserted_ids = []

    for i, run in enumerate(runs):
        # Get pre-calculated field references and substitute object_key
        field_refs = field_references_list[i]
        for field_name in field_refs:
            if field_refs[field_name]:
                field_refs[field_name] = field_refs[field_name].format(object_key=object_key)

        run_id = await db.fetchval(
            """
            INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            run.id,
            run.trace_id,
            run.name,
            field_refs["inputs"],
            field_refs["outputs"],
            field_refs["metadata"],
        )
        inserted_ids.append(str(run_id))

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

    # Function to parse S3 reference
    def parse_s3_ref(ref):
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

    # Function to fetch data from S3 based on reference with byte range
    async def fetch_from_s3(ref):
        if not ref or not ref.startswith("s3://"):
            return {}

        bucket, key, offsets, field = parse_s3_ref(ref)
        if not bucket or not key or not offsets:
            return {}

        start_offset, end_offset = offsets
        byte_range = f"bytes={start_offset}-{end_offset-1}"

        try:
            # Fetch only the required byte range
            response = await s3.get_object(Bucket=bucket, Key=key, Range=byte_range)
            async with response["Body"] as stream:
                data = await stream.read()
            try:
                # The data should be a valid JSON object corresponding to the field
                # (inputs, outputs, or metadata) without needing further extraction
                return orjson.loads(data)
            except Exception as parse_error:
                print(f"Error parsing JSON fragment: {parse_error}")
                print(f"Problematic data: {data}")
                return {}

        except Exception as e:
            print(f"Error fetching S3 object with range: {e}")
            return {}

    inputs, outputs, metadata = await asyncio.gather(
        fetch_from_s3(run_data["inputs"]),
        fetch_from_s3(run_data["outputs"]),
        fetch_from_s3(run_data["metadata"]),
    )

    return {
        "id": str(run_data["id"]),
        "trace_id": str(run_data["trace_id"]),
        "name": run_data["name"],
        "inputs": inputs,
        "outputs": outputs,
        "metadata": metadata,
    }
