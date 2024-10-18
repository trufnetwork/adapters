# TSN Adapters

This repository contains examples of how to use the TSN SDK to ingest data from various sources into TSN. It leverages [truflation/truflation](https://github.com/truflation/truflation) connectors to get data from sources, powered by [Prefect](https://www.prefect.io/) features such as resiliency, concurrency, caching, observability, UI controls and more.

Each [example](examples) is a Prefect flow that can be run locally or in a remote environment.

## Reusable Tasks

We have a few reusable tasks that can be used to ingest data from various sources into TSN. They can serve as an example on how to use the TSN SDK to achieve certain tasks, or may also be reused in your own flows (you may want to install this package to use them).

Non-exhaustive list:

- [x] [Read a CSV file from a GitHub repository](tsn_adapters/tasks/github.py)
- [x] [Read a Google Sheet](tsn_adapters/tasks/gsheet.py)
- [x] [Insert records into TSN](tsn_adapters/tasks/tsn.py)
- [x] [Get all records from TSN](tsn_adapters/tasks/tsn.py)
- [x] [Deploy a primitive](tsn_adapters/tasks/tsn.py)
- [x] [Reconcile data](tsn_adapters/tasks/data_manipulation.py)

## Examples

### Simple GSheets

We have two versions of the flow:

1. [Direct](examples/gsheets/direct/direct_flow.py) - In this version, we directly specify the sheet ID and the source ID to filter by.
2. [Dynamic](examples/gsheets/dynamic/dynamic_flow.py) - In this version, we fetch the sheet ID and the source ID from a CSV file in a GitHub repository.