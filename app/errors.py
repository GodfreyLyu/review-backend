from enum import StrEnum


class ValidationReason(StrEnum):
    MISSING_SECTIONS = "missing_sections"
    SECTION_ORDER = "section_order"
    EMPTY_SECTION = "empty_section"
    UNEXPECTED_SECTION_HEADING = "unexpected_section_heading"
    DETACHED_SOURCE = "detached_source"
    TRUNCATED_SECTION = "truncated_section"


class AppError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        status: int = 400,
        *,
        validation_reason: ValidationReason | None = None,
    ):
        if validation_reason is not None and not isinstance(validation_reason, ValidationReason):
            raise TypeError("validation_reason must be a ValidationReason")
        self.code = code
        self.message = message
        self.status = status
        self.validation_reason = validation_reason
        super().__init__(message)


class InferenceUnavailable(AppError):
    """The dependency is unavailable; callers must still reconcile any admitted request."""

    def __init__(self):
        super().__init__(
            "inference_unavailable",
            "The inference service is temporarily unavailable. Please retry shortly.",
            503,
        )


class InferenceUncertain(AppError):
    """A remote call lost its execution state; admission must fail closed."""

    def __init__(self):
        super().__init__(
            "inference_stuck",
            "Inference state could not be confirmed. The service needs recovery.",
            503,
        )
