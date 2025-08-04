"""
Pydantic models for runs.
"""
import uuid
from typing import Any, Dict, Optional

from pydantic import UUID4, BaseModel, Field


class Run(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid.uuid4)
    trace_id: UUID4
    name: str
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}