from prefect import flow
import pytest
from trufnetwork_sdk_py.utils import generate_stream_id

from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.flows.stream_deploy_flow import task_check_exist_deploy_init

# Marks all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.usefixtures("prefect_test_fixture", "tn_node") # Ensure Prefect and TSN node are ready
def test_task_check_exist_deploy_init_handles_already_deployed(tn_block: TNAccessBlock):
    """
    Integration test for task_check_exist_deploy_init.
    Verifies that the task correctly handles a stream that is already deployed
    by proceeding to initialize it without raising an error.
    Uses the real tn_block fixture.
    """
    stream_id = generate_stream_id("integ_test_already_deployed")
    print(f"Generated stream ID for test: {stream_id}") # Debug output

    try:
        # --- Setup: Deploy the stream first ---
        print(f"Deploying stream {stream_id}...")
        deploy_tx = tn_block.deploy_stream(stream_id, wait=True)
        print(f"Stream {stream_id} deployed. Tx: {deploy_tx}")

        # --- Execute the task within a flow ---
        @flow
        def test_flow_wrapper(block: TNAccessBlock):
            print(f"Running task_check_exist_deploy_init for {stream_id}...")
            # Call the task directly (since it's already decorated with @task)
            status = task_check_exist_deploy_init(stream_id=stream_id, tna_block=block)
            print(f"Task returned status: {status}")
            return status

        # Run the flow with the real block
        result_status = test_flow_wrapper(block=tn_block)

        # --- Assert ---
        # If deploy already happened, the task should recognize this and proceed to init.
        # The final status should indicate initialization occurred or was confirmed.
        assert result_status == "initialized"
        print(f"Assertion passed: Status is '{result_status}' as expected.")

        # Verify initialization actually happened (e.g., check for first record)
        try:
            stream_type = tn_block.get_stream_type(tn_block.current_account, stream_id)
            assert stream_type == "primitive"
            print(f"Verified: Stream {stream_id} is initialized.")
        except Exception as e:
            pytest.fail(f"Failed to verify stream initialization for {stream_id}: {e}")


    finally:
        # --- Cleanup ---
        print(f"Cleaning up stream {stream_id}...")
        try:
            tn_block.destroy_stream(stream_id, wait=True)
            print(f"Stream {stream_id} destroyed successfully.")
        except Exception as e:
            # Log cleanup error but don't fail the test if the main logic passed
            print(f"Warning: Failed to destroy stream {stream_id} during cleanup: {e}")


@pytest.mark.usefixtures("prefect_test_fixture", "tn_node")
def test_task_check_exist_deploy_init_handles_already_initialized(tn_block: TNAccessBlock):
    """
    Integration test for task_check_exist_deploy_init.
    Verifies that the task correctly handles a stream that is already deployed *and* initialized
    by recognizing the existing state without raising an error.
    Uses the real tn_block fixture.
    """
    stream_id = generate_stream_id("integ_test_already_initialized")
    print(f"Generated stream ID for test: {stream_id}")

    try:
        # --- Setup: Deploy and Initialize the stream first ---
        print(f"Deploying stream {stream_id}...")
        deploy_tx = tn_block.deploy_stream(stream_id, wait=True)
        print(f"Stream {stream_id} deployed. Tx: {deploy_tx}")

        print(f"Initializing stream {stream_id}...")
        init_tx = tn_block.init_stream(stream_id, wait=True)
        print(f"Stream {stream_id} initialized. Tx: {init_tx}")

        # --- Execute the task within a flow ---
        @flow
        def test_flow_wrapper(block: TNAccessBlock):
            print(f"Running task_check_exist_deploy_init for already initialized {stream_id}...")
            status = task_check_exist_deploy_init(stream_id=stream_id, tna_block=block)
            print(f"Task returned status: {status}")
            return status

        # Run the flow with the real block
        result_status = test_flow_wrapper(block=tn_block)

        # --- Assert ---
        # If deploy and init already happened, the task should recognize this.
        # The final status should indicate it's already initialized.
        assert result_status == "initialized"
        print(f"Assertion passed: Status is '{result_status}' as expected for already initialized stream.")

    finally:
        # --- Cleanup ---
        print(f"Cleaning up stream {stream_id}...")
        try:
            tn_block.destroy_stream(stream_id, wait=True)
            print(f"Stream {stream_id} destroyed successfully.")
        except Exception as e:
            print(f"Warning: Failed to destroy stream {stream_id} during cleanup: {e}") 