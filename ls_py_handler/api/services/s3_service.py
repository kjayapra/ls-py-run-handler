"""
S3 service for optimized field retrieval and batch processing.
"""
from typing import Any, Dict, List
import orjson


def parse_s3_ref(ref: str):
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


async def fetch_all_fields_optimized(s3: Any, field_refs: Dict[str, str]) -> Dict[str, Any]:
    """
    Fetch all fields with minimal S3 requests by consolidating byte ranges.
    Reduces 3 S3 calls to 1 call per S3 object.
    """
    # Group references by S3 object
    s3_objects = {}

    for field_name, ref in field_refs.items():
        if ref and ref.startswith("s3://"):
            bucket, key, offsets, _field = parse_s3_ref(ref)
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


async def fetch_multiple_runs_optimized(s3: Any, runs_data: List[Dict]) -> List[Dict[str, Any]]:
    """
    Fetch multiple runs with optimized S3 field consolidation.
    Groups S3 requests by object and consolidates byte ranges.
    """
    if not runs_data:
        return []

    # Group all field references by S3 object across all runs
    s3_objects = {}

    for run_idx, run_data in enumerate(runs_data):
        field_refs = {
            "inputs": run_data["inputs"],
            "outputs": run_data["outputs"], 
            "metadata": run_data["metadata"]
        }

        # Process each field reference
        for field_name, ref in field_refs.items():
            if ref and ref.startswith("s3://"):
                bucket, key, offsets, _field = parse_s3_ref(ref)
                if bucket and key and offsets:
                    if key not in s3_objects:
                        s3_objects[key] = {
                            "bucket": bucket,
                            "fields": {},
                            "min_start": float("inf"),
                            "max_end": 0,
                        }

                    start_offset, end_offset = offsets
                    field_key = f"{run_idx}_{field_name}"
                    s3_objects[key]["fields"][field_key] = {
                        "start": start_offset,
                        "end": end_offset,
                        "run_idx": run_idx,
                        "field_name": field_name,
                    }
                    s3_objects[key]["min_start"] = min(
                        s3_objects[key]["min_start"], start_offset
                    )
                    s3_objects[key]["max_end"] = max(
                        s3_objects[key]["max_end"], end_offset
                    )

    # Initialize results for all runs
    results = []
    for run_data in runs_data:
        results.append({
            "id": str(run_data["id"]),
            "trace_id": str(run_data["trace_id"]),
            "name": run_data["name"],
            "inputs": {},
            "outputs": {},
            "metadata": {}
        })

    # Fetch consolidated S3 data and populate results
    for key, obj_info in s3_objects.items():
        try:
            start = obj_info["min_start"]
            end = obj_info["max_end"]
            byte_range = f"bytes={start}-{end-1}"

            response = await s3.get_object(
                Bucket=obj_info["bucket"], Key=key, Range=byte_range
            )
            async with response["Body"] as stream:
                consolidated_data = await stream.read()

            # Extract individual fields and assign to correct runs
            for field_key, field_info in obj_info["fields"].items():
                field_start = field_info["start"] - start
                field_end = field_info["end"] - start
                field_data = consolidated_data[field_start:field_end]

                try:
                    parsed_data = orjson.loads(field_data)
                    run_idx = field_info["run_idx"]
                    field_name = field_info["field_name"]
                    results[run_idx][field_name] = parsed_data
                except Exception as parse_error:
                    print(f"Error parsing field {field_key}: {parse_error}")

        except Exception as e:
            print(f"Error fetching S3 object {key}: {e}")

    return results