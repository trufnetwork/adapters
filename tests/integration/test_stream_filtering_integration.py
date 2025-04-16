from typing import Any
import pytest
from tsn_adapters.blocks.tn_access import TNAccessBlock, StreamLocatorModel, convert_to_typed_df
from tsn_adapters.blocks.stream_filtering import task_get_stream_states_divide_conquer
import pandas as pd
from trufnetwork_sdk_py.utils import generate_stream_id


@pytest.mark.integration
def test_stream_state_partitioning_with_real_tn(tn_block: TNAccessBlock):
    """
    Integration test: Verifies stream state partitioning against a real TN node.

    - Deploys a new stream (should be non-deployed at first).
    - Initializes it (should become deployed and initialized).
    - Checks state partitioning at each step.
    - Cleans up after itself.
    """
    # Arrange
    data_provider = tn_block.current_account
    stream_id = generate_stream_id("testintegrationstream00000000000001")
    locators = pd.DataFrame([{"data_provider": data_provider, "stream_id": stream_id}])
    locators_typed = convert_to_typed_df(locators, StreamLocatorModel)

    # Ensure stream does not exist
    try:
        tn_block.destroy_stream(stream_id, wait=True)
    except Exception:
        pass  # Ignore if not found

    # Act & Assert: 1. Should be non-deployed
    result = task_get_stream_states_divide_conquer.fn(
        block=tn_block,
        stream_locators=locators_typed,
        max_depth=2,
        current_depth=0,
    )
    assert len(result["non_deployed"]) == 1
    assert result["non_deployed"].iloc[0]["stream_id"] == stream_id

    # Act: Deploy the stream (but do not initialize)
    tn_block.deploy_stream(stream_id, wait=True)

    # Assert: Should be deployed but not initialized
    result2 = task_get_stream_states_divide_conquer.fn(
        block=tn_block,
        stream_locators=locators_typed,
        max_depth=2,
        current_depth=0,
    )
    assert len(result2["deployed_but_not_initialized"]) == 1
    assert result2["deployed_but_not_initialized"].iloc[0]["stream_id"] == stream_id

    # Act: Initialize the stream
    tn_block.init_stream(stream_id, wait=True)

    # Assert: Should be deployed and initialized
    result3 = task_get_stream_states_divide_conquer.fn(
        block=tn_block,
        stream_locators=locators_typed,
        max_depth=2,
        current_depth=0,
    )
    assert len(result3["deployed_and_initialized"]) == 1
    assert result3["deployed_and_initialized"].iloc[0]["stream_id"] == stream_id

    # Cleanup
    tn_block.destroy_stream(stream_id, wait=True)