# Argentina SEPA Ingestion Module

This module handles data ingestion and processing for the Argentina SEPA dataset using Prefect flows, tasks, and Pandera schemas.

## Architecture Overview

This module implements a generic pipeline architecture designed for reuse across different data sources:
- **Interfaces**: Core abstractions (`interfaces/`) for data providers, reconciliation strategies, and target systems
- **Implementations**: Concrete SEPA-specific components that implement these interfaces
- **Flow Orchestration**: Prefect tasks and flows coordinating the entire pipeline

## Directory Structure

- **`aggregate/`**: Category-level price aggregation logic
- **`flows/`**: Pipeline orchestration and entry points
- **`interfaces/`**: Abstract interfaces for pipeline components
- **`models/`**: Pandera schemas for data validation
- **`scrapers/`**: SEPA-specific data extraction
- **`utils/`**: Shared helper functions

**Key Files:**
- `provider.py`: Data retrieval implementation
- `reconciliation.py`: Data reconciliation strategies
- `target.py`: TrufNetwork integration
- `task_wrappers.py`: Prefect task implementations

## Usage

1. **Setup**: Configure Prefect blocks (`TNAccessBlock`, `PrimitiveSourcesDescriptor`)
2. **Configure**: Set `source_descriptor_type`, block names, and category map URL
3. **Run**: Execute `flows/argentina_ingestor_flow.py` or create a custom deployment
4. **Monitor**: Check Prefect artifacts for logs and summaries

## Extending

- Implement `interfaces/` for new data sources. We plan to extract these into a top level `interfaces/` folder in the next iterations.
- Add custom reconciliation strategies via `IReconciliationStrategy`
- Update Pandera models for new data schemas
