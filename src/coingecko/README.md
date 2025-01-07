# CoinGecko Adapter

## Fetch And Push Records Service

Description of the fetch and push records service goes here.

---

## Coin List Fetcher

### Overview

**Coin List Fetcher** is a Go-based utility designed to fetch a list of cryptocurrencies from the [CoinGecko API](https://www.coingecko.com/en/api) and export essential details (`id` and `symbol`) into a CSV file. This CSV serves as a consistent and static reference for further development, ensuring that the coin list remains unchanged over time. By utilizing this approach, you avoid inconsistencies that may arise from dynamic changes in coin rankings or availability.

### Features

- **Single-Time Fetch:** Retrieves the coin list once and maintains a consistent CSV file for future reference.
- **Environment Configuration:** Securely manages the CoinGecko API key using a `.env` file.
- **Task Automation:** Leverages `Taskfile.yml` for streamlined build and run processes.
- **CSV Output:** Generates a `coins.csv` containing only the `id` and `symbol` of each cryptocurrency.

### Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
    - [1. Configure Environment Variables](#1-configure-environment-variables)
- [Usage](#usage)
    - [Using Taskfile](#using-taskfile)

### Prerequisites

Before getting started, ensure you have the following installed on your machine:

- **Go:** Download from [Go Downloads](https://golang.org/dl/).
- **Task:** A task runner similar to Make. Install from [Task Releases](https://github.com/go-task/task/releases).
- **Git:** For cloning the repository. Download from [Git Downloads](https://git-scm.com/downloads).

### Setup

#### Configure Environment Variables

Create a `.env` file based on the provided `.env.example` template. This file will store your CoinGecko API key securely.

```bash
cp .env.example .env
```

Open the `.env` file in your preferred text editor and add the following line:

```env
COINGECKO_API_KEY=your_api_key_here
```

**Important:** Replace `your_api_key_here` with your actual CoinGecko API key. If you don't have one, you can obtain it by [registering on CoinGecko](https://www.coingecko.com/en/api).

### Usage

The project utilizes a `Taskfile.yml` to automate common tasks such as building binaries and generating the CSV file.

#### Using Taskfile

1. **Generate the Coin List CSV**

To fetch the coin list from CoinGecko and generate the `coins.csv` file, run:

```bash
task coin_list
```

**Description:** This task executes the Go program that fetches the coin data and writes the `id` and `symbol` to `coins.csv`.

2. **Build the Binary**

To compile the Go program into a binary executable, run:

```bash
task build:coin_list
```

**Description:** This task builds the Go application and outputs the binary to the `./.build/` directory.