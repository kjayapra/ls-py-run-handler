"""
Batch processing service for optimized JSON streaming and database operations.
"""
import uuid
from typing import Any, Dict, List
import orjson

from ls_py_handler.config.settings import settings


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