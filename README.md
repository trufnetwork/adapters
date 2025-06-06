# TN Adapters

Adapters facilitate the ingestion of data from various sources into TN, leveraging the SDK and Prefect for efficient data management.

> **Note**: While adapters use a Python Framework, it's also possible to push data from other languages like Go or JavaScript. Refer to the respective SDKs for more information:
>
> - [Golang SDK](https://github.com/trufnetwork/sdk-go)
> - [JavaScript SDK](https://github.com/trufnetwork/sdk-js)
>

## Prerequisites

- **Programming Knowledge**: 
  - Proficiency in Python
  - Prefect knowledge (https://www.prefect.io)
- **Tools**:
  - [Docker](https://www.docker.com/get-started)
  - [Prefect](https://www.prefect.io)
  - Access to a TN Node (local or remote). You can refer to documentation [here](https://github.com/trufnetwork/node/blob/main/docs/development.md). 
  TLDR: you can run `task single:start` from that repository after you clone it.

## Setting Up the Development Environment

1. **Clone the Repository**:

```bash
git clone https://github.com/trufnetwork/adapters.git
cd adapters
```

2. **Configure Environment Variables**:

- Duplicate the `.env.example` file and rename it to `.env`.
- Update the environment variables in the `.env` file as needed.

3. **Setup Virtual Environment**:

We recommend using a virtual environment to manage dependencies. You can create one using the following commands:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ."[dev]"
```

4. **Launch Services with Docker Compose**:

```bash
docker compose up -d
```

This command initializes the necessary services in the background.

5. **Browser:**

If you prefer to see the flows and blocks in browser, you can access http://localhost:4200/dashboard, on your local machine.

## Understanding Reusable Tasks

The repository offers several reusable tasks to facilitate data ingestion into TN. However you can develop your own tasks to fit your *use-case*, these tasks can serve as example how to implement custom adapter to your data:

**Reading Data**:
- [`read_repo_csv_file`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/tsn_adapters/blocks/github_access.py#L48): Read a CSV file from a GitHub repository.
- [`task_read_gsheet`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/tsn_adapters/tasks/gsheet.py#L11): Retrieves data from a Google Sheet.

**TN Operations**:
- [`task_insert_tsn_records`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/tsn_adapters/common/trufnetwork/tn.py#L28): Inserts records into TN.
- [`task_get_all_tsn_records`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/tsn_adapters/common/trufnetwork/tn.py#L106): Fetches all records from TN.
- [`task_deploy_primitive_if_needed`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/examples/gsheets/utils.py#L27): Deploys a primitive if it doesn't already exist.

**Data Manipulation**:
- [`task_reconcile_data`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/tsn_adapters/tasks/data_manipulation.py#L6): Reconciles data between sources.
- [`task_normalize_source`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/examples/gsheets/utils.py#L47): Standardizes source data.
- [`task_filter_by_source_id`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/examples/gsheets/utils.py#L5): Filters data based on a source ID.
- [`task_prepare_records_for_tsn`](https://github.com/trufnetwork/adapters/blob/c4788dbd513265edd2a8a7ff987542946632dacd/src/examples/gsheets/utils.py#L14): Prepares records for insertion into TN.

## Examples

### Ingesting Data from Google Sheets

The repository includes examples demonstrating data ingestion from Google Sheets:

- **Direct Method**: Specify the sheet ID and source ID directly.
- **Dynamic Method**: Retrieve the sheet ID and source ID from a CSV file in a GitHub repository.

Refer to the [examples directory](https://github.com/trufnetwork/adapters/tree/main/src/examples/gsheets) for detailed implementations.

## Additional Resources

- [Python SDK](https://github.com/trufnetwork/sdk-py)
- [Golang SDK](https://github.com/trufnetwork/sdk-go)
- [JavaScript SDK](https://github.com/trufnetwork/sdk-js)
- [Prefect Documentation](https://docs.prefect.io)
