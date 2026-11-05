"""Application review budgets. Model execution policy belongs to inference-service."""

from pydantic import BaseModel, Field


class ModelSettings(BaseModel):
    expected_model_id: str | None = None
    expected_model_revision: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    model_max_output_tokens: int = Field(default=384, ge=3, le=32768)
    model_temperature: float | None = Field(default=None, ge=0, le=2)
    model_top_p: float | None = Field(default=None, gt=0, le=1)
    inference_timeout_seconds: float = Field(default=300, gt=0, le=600)
