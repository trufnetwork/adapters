"""EPS Stream Deployment Flow.

Deploys the 21 primitive streams required for EPS ingestion on TN:
  7 tickers × 3 stream types (fmp_eps, yahoo_eps, truf_eps) = 21 streams

One-shot and idempotent — already-deployed streams are skipped automatically
by deploy_streams_flow. Run this before eps_historical_flow or
eps_real_time_flow.

Usage:
    from tsn_adapters.flows.eps.deploy_flow import eps_deploy_flow
    from tsn_adapters.blocks.tn_access import TNAccessBlock

    eps_deploy_flow(tn_block=TNAccessBlock.load("default"))
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.flows.stream_deploy_flow import DeployStreamResults, deploy_streams_flow
from tsn_adapters.tasks.eps.config import (
    EPS_STREAM_IDS,
    FMP_EPS_STREAM_IDS,
    MAG7,
    YAHOO_EPS_STREAM_IDS,
    fmp_eps_stream_name,
    truf_eps_stream_name,
    yahoo_eps_stream_name,
)

ALL_EPS_SOURCE_TYPES: tuple[str, ...] = ("fmp_eps", "yahoo_eps", "truf_eps")


class EpsSourceDescriptor(PrimitiveSourcesDescriptorBlock):
    """In-memory descriptor for EPS streams — no external storage needed.

    Generates EPS stream definitions (MAG7 × `source_types`) deterministically
    from config. Stream IDs are computed via generate_stream_id and never
    change for a given ticker/convention pair.

    `source_types` selects which of {"fmp_eps", "yahoo_eps", "truf_eps"} to
    include — useful when each source identity lives on its own TN wallet
    and deployment must be routed accordingly.
    """

    _block_type_name = "EPS Source Descriptor"
    _block_type_slug = "eps-source-descriptor"

    source_types: tuple[str, ...] = ALL_EPS_SOURCE_TYPES

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        rows = []
        for symbol in MAG7:
            if "fmp_eps" in self.source_types:
                rows.append({
                    "stream_id": FMP_EPS_STREAM_IDS[symbol],
                    "source_id": symbol,
                    "source_type": "fmp_eps",
                    "source_display_name": fmp_eps_stream_name(symbol),
                })
            if "yahoo_eps" in self.source_types:
                rows.append({
                    "stream_id": YAHOO_EPS_STREAM_IDS[symbol],
                    "source_id": symbol,
                    "source_type": "yahoo_eps",
                    "source_display_name": yahoo_eps_stream_name(symbol),
                })
            if "truf_eps" in self.source_types:
                rows.append({
                    "stream_id": EPS_STREAM_IDS[symbol],
                    "source_id": symbol,
                    "source_type": "truf_eps",
                    "source_display_name": truf_eps_stream_name(symbol),
                })
        return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(rows))


@flow(name="EPS Stream Deployment Flow")
def eps_deploy_flow(
    tn_block: TNAccessBlock,
    source_types: tuple[str, ...] = ALL_EPS_SOURCE_TYPES,
    deployment_state: Optional[DeploymentStateBlock] = None,
) -> DeployStreamResults:
    """Deploy EPS primitive streams to TN.

    Streams written go to `tn_block.current_account`. Pass `source_types`
    to restrict the descriptor to a single source identity per wallet
    (e.g. `("fmp_eps",)` on the FMP wallet block).

    Idempotent — already-deployed streams are skipped. Pass a
    DeploymentStateBlock to persist deployment state across runs and
    avoid redundant TN existence checks.
    """
    descriptor = EpsSourceDescriptor(source_types=source_types)
    return deploy_streams_flow(
        psd_block=descriptor,
        tna_block=tn_block,
        deployment_state=deployment_state,
    )


if __name__ == "__main__":
    import asyncio
    from tsn_adapters.utils import deroutine

    async def main() -> None:
        tn_block = deroutine(TNAccessBlock.load("default"))
        result = eps_deploy_flow(tn_block=tn_block)
        print(f"Deployed: {result['deployed_count']}, Skipped: {result['skipped_count']}")

    asyncio.run(main())
