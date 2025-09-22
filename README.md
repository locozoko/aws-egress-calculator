# AWS Egress Calculator

`aws-egress-calculator.py` is a Python utility for analyzing **AWS NAT Gateway traffic** and optionally **Data Transfer Out (DTO)** across one or more AWS accounts.  
It supports single-account mode or multi-account mode via **AWS Organizations** + cross-account IAM role assumption.

A sample output file is included in this repository:  
[`aws-output-SAMPLE.txt`](aws-output-SAMPLE.txt)

Right-click and open in new tab to watch the demo video:
[![AWS Egress Calculator Demo Video](https://cdn.loom.com/sessions/thumbnails/a6f3270e3e5947e78e9d39a8f12990d1-ee6a834587ceee13-full-play.gif)](https://www.loom.com/share/a6f3270e3e5947e78e9d39a8f12990d1?sid=e52db9d2-d0d1-4c3a-94bd-91788fa11a0e)

---

## Features

- Collects **NAT Gateway Upload/Download/Total GB** usage for the last 12 months  
- Optionally collects **DTO (Data Transfer Out)** usage from Cost Explorer  
- Supports:
  - **Single Account Mode** (current caller’s account)  
  - **Organization Mode** (iterates through accounts, assuming a specified role)  
- Interactive prompts for:
  - AWS Org mode (`y/n`)  
  - Include DTO metrics (`y/n`)  
  - Region selection (single or all)  
  - IAM role name (for Org mode)  
  - Option to resume from cached runs  
  - Option to print detailed tables  
- **Resumable runs** via cached CSVs (`nat_org_usage_cache.csv`, `dto_usage_cache.csv`)  
- Saves output to timestamped file (`aws-output-YYYYMMDD-HHMM.txt`)  
- Cleans up cache files on successful run

---

## Requirements

- **Python 3.9+** (tested with Homebrew Python 3.13)  
- AWS CLI v2 (for SSO login support)  
- Valid AWS credentials:
  - Single-account mode → credentials for that account  
  - Org mode → caller must have `sts:AssumeRole` permission into the specified role in each target account  
- Packages listed in [`aws-requirements.txt`](aws-requirements.txt):
  - `boto3`  
  - `pandas`  
  - `python-dateutil`

---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/locozoko/aws-egress-calculator.git
cd aws-egress-calculator
```

### 2. Create a virtual environment (recommended)
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install requirements
```bash
pip install -r aws-requirements.txt
```
## Usage

Run the script:
```bash
python aws-egress-calculator.py
```

You will be prompted for:
- Whether to run against AWS Organziations
- Wehter to include DTO metrics
- Which AWS region(s) to analyze
- IAM role name to assume (if Org mode is enabled)

## Example Run
```bash
  $ python aws-egress-calculator.py

  [✓] Output will also be saved to: aws-output-20250922-1015.txt
  
  Run against AWS Organizations? (y/n): y
  Include DTO (Data Transfer Out) metrics? (y/n): y
  
  Select a region to query:
  0) All regions
  1) us-east-1
  2) us-west-2
  3) ...
  Enter choice: 0
  
  Enter IAM role name to assume in each account: demoDtoMetricsReaderRole
```

### Options / Prompts
- Org mode (y/n)
  - y: Enumerates accounts via AWS Organizations API and assumes role
  - n: Uses only the current AWS account
- Include DTO (y/n)
  - Adds Cost Explorer DTO usage queries
  - Note: DTO is only fully accurate from the Org management (payer) account
- Region selection
  - Choose “All regions” or a specific region
- Resume mode
  - If cache files exist, you can resume from last run instead of re-querying
- Detailed tables (y/n)
  - Print per-account, per-region, per-month traffic breakdown tables
 
### Sample Output
See [`aws-output-SAMPLE.txt`](aws-output-SAMPLE.txt) for an example. Outputs include:
- Summary totals (NAT, DTO)
- A DTO vs NAT egress comparison (if DTO enabled)
- Per-account / per-region breakdown (if detailed tables enabled)
- 
---

## Notes
- When using Org mode, ensure each target account has a role with the proper trust policy to allow assumption from your caller account
- If using Org mode, if the permissions to ListAccounts do not exist, the script automatically drops to non-Org mode
- NAT and DTO suage are cached during a run; cahces are deleted after a successful run
- This script is not officially supported by or maintained by Zscaler. This project was created due to customer demand for getting help getting egress traffic
- If a run fails, you can resume later without re-querying prior months
