from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from prefect.artifacts import create_markdown_artifact
from tsn_adapters.tasks.argentina.errors.errors import ArgentinaSEPAError

error_ctx = ContextVar("argentina_sepa_errors")

class ErrorAccumulator:
    def __init__(self):
        self.errors = []

    def add_error(self, error: ArgentinaSEPAError):
        self.errors.append(error)

    def model_dump(self):
        return [error.to_dict() for error in self.errors]
    
    def model_load(self, data: list[dict[str, Any]]):
        self.errors = [ArgentinaSEPAError(**error) for error in data]

    @classmethod
    def get_or_create_from_context(cls) -> 'ErrorAccumulator':
        errors = error_ctx.get()
        if errors is None:
            errors = ErrorAccumulator()
            errors.set_to_context()
        return errors

    def set_to_context(self):
        error_ctx.set(self)


@contextmanager
def error_collection():
    """Context manager for collecting errors during processing."""
    accumulator = ErrorAccumulator()
    accumulator.set_to_context()
    try:
        yield accumulator
    finally:
        # Create error summary if there are errors
        if accumulator.errors:
            error_summary = [
                "# Processing Errors\n",
                "The following errors occurred during processing:\n",
            ]
            for error in accumulator.errors:
                error_dict = error.to_dict()
                error_summary.extend(
                    [
                        f"\n## {error_dict['code']}: {error_dict['message']}\n",
                        f"Responsibility: {error_dict['responsibility'].value}\n",
                        "Context:\n",
                        "```json\n",
                        f"{error_dict['context']}\n",
                        "```\n",
                    ]
                )

            # Create markdown artifact with the error summary
            create_markdown_artifact(
                key="processing-errors",
                markdown="".join(error_summary),
                description="Errors encountered during SEPA data processing",
            )
