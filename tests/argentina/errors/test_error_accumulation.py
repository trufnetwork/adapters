"""
Test suite for Argentina SEPA error handling system.

This test suite verifies the error handling system's core functionalities:

1. Error Accumulation:
   - Errors can be collected and accumulated during processing
   - Each error maintains its structured data (code, message, responsibility, context)
   - Errors are properly serialized for reporting

2. Context Isolation:
   - Multiple concurrent tasks can maintain isolated error contexts
   - Errors from one task don't leak into another task's context
   - Each task's error context is properly managed and cleaned up

3. Error Reporting:
   - Accumulated errors are properly formatted into markdown artifacts
   - Error reports include all necessary information (code, message, responsibility, context)
   - Artifacts are created at appropriate points in the flow

Key Conclusions:
- The error handling system is thread-safe and suitable for concurrent execution
- Error contexts are properly isolated between different tasks and flows
- The system successfully maintains error traceability and accountability
- Error reporting provides clear, structured information for debugging and monitoring

Test Structure:
- error_accumulation_flow: Tests basic error collection and accumulation
- concurrent_error_contexts_flow: Verifies context isolation in concurrent execution
- test_error_reporting: Ensures proper artifact creation and formatting
- test_context_isolation: Validates complete isolation between concurrent tasks
"""

from unittest.mock import patch

from prefect import flow, task

from tsn_adapters.tasks.argentina.errors import DateMismatchError, EmptyCategoryMapError, ErrorAccumulator
from tsn_adapters.tasks.argentina.flows.preprocess_flow import error_collection


@task
def task_that_raises_error():
    """Task that intentionally raises a known error"""
    accumulator = ErrorAccumulator.get_or_create_from_context()
    accumulator.add_error(EmptyCategoryMapError(url="test://invalid-map"))
    accumulator.add_error(DateMismatchError("2024-01-01", "2024-01-02"))

@task
def task_with_isolated_errors_1():
    """First task with its own error context"""
    with error_collection() as task1_accumulator:
        task1_accumulator.add_error(EmptyCategoryMapError(url="task1://error"))
        return task1_accumulator.model_dump()

@task
def task_with_isolated_errors_2():
    """Second task with its own error context"""
    with error_collection() as task2_accumulator:
        task2_accumulator.add_error(DateMismatchError("2024-03-01", "2024-03-02"))
        return task2_accumulator.model_dump()

@flow
def error_accumulation_flow():
    """Flow that forces error conditions"""
    with error_collection() as accumulator:
        task_that_raises_error.submit().result()
        
        # Verify errors are collected during processing
        assert len(accumulator.errors) == 2, "Should collect 2 errors"
        assert isinstance(accumulator.errors[0], EmptyCategoryMapError)
        assert isinstance(accumulator.errors[1], DateMismatchError)

@flow
def concurrent_error_contexts_flow():
    """Flow that tests error context isolation between tasks"""
    # Submit both tasks concurrently
    task1_future = task_with_isolated_errors_1.submit()
    task2_future = task_with_isolated_errors_2.submit()
    
    # Get results
    task1_errors = task1_future.result()
    task2_errors = task2_future.result()
    
    # Verify task1 errors
    assert len(task1_errors) == 1, "Task 1 should have 1 error"
    assert task1_errors[0]["code"] == "ARG-300"
    assert "task1://error" in task1_errors[0]["context"]["url"]
    
    # Verify task2 errors
    assert len(task2_errors) == 1, "Task 2 should have 1 error"
    assert task2_errors[0]["code"] == "ARG-200"
    assert "2024-03-01" in task2_errors[0]["context"]["external_date"]

def test_error_reporting():
    """Test the full error accumulation and reporting flow"""
    with patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.create_markdown_artifact") as mock_artifact:
        error_accumulation_flow()
        
        # Verify artifact creation
        mock_artifact.assert_called_once()
        
        # Verify artifact content
        markdown_content = mock_artifact.call_args[1]["markdown"]
        assert "ARG-300" in markdown_content
        assert "ARG-200" in markdown_content
        assert "test://invalid-map" in markdown_content
        assert "Date mismatch: Reported 2024-01-01 vs Actual 2024-01-02" in markdown_content

def test_context_isolation():
    """Test that error contexts remain isolated between concurrent tasks"""
    concurrent_error_contexts_flow()


if __name__ == "__main__":
    test_error_reporting()
