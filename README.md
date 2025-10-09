# AWS Scripts

This repository contains a collection of scripts designed for AWS resource management, including a powerful resource inventory tool and utilities for managing Elastic Network Adapter (ENA) drivers.

## Scripts Overview

### 1. AWS Resource Inventory

This is the core tool of the repository, designed to scan and inventory resources across multiple AWS accounts and regions.

- **`AWS-list-resources-all-canary.py`**: The development (canary) version of the script. It contains the latest features, including the most recently added service collectors like AWS Lightsail.
- **`AWS-list-resources-all-rc.py`**: The release candidate (RC) version of the script. It represents a more stable version of the inventory tool.

**Features:**

- Scans specified AWS accounts by assuming an IAM role (e.g., `OrganizationAccountAccessRole`).
- Collects detailed information from a wide range of AWS services, including EC2, S3, RDS, IAM, Lambda, VPC, WAF, and Lightsail.
- Generates a comprehensive Excel workbook (`aws_inventory.xlsx`) with a separate sheet for each service inventoried.
- Provides a summary sheet with resource counts per account and service.
- Supports filtering by account ID and AWS region.
- Applies adaptive retries for throttling and transient network errors so large multi-account scans finish reliably.
- Discovers undocumented pagination tokens dynamically to avoid truncating inventories when AWS adds new paging fields.

**Usage:**

```bash
# Scan the current account for all resources in all enabled regions
python3 AWS-list-resources-all-canary.py

# Scan multiple accounts from a management account, specifying a role and regions
python3 AWS-list-resources-all-canary.py --master --role-name MyRole --regions us-east-1,us-west-2

# Scan specific accounts
python3 AWS-list-resources-all-canary.py --master --include-accounts "123456789012,987654321098"

# Provide an external ID and custom role session duration when assuming roles
python3 AWS-list-resources-all-canary.py \
  --master \
  --include-accounts "123456789012" \
  --external-id "my-shared-secret" \
  --assume-role-duration 3600
````

### 2\. S3 Presigned URL Script Runner

These wrapper scripts are designed to securely download and execute scripts (like the inventory tool) from an S3 bucket using a presigned URL.

- **`run-presigned-canary.sh`**: A robust script that can download and run Python, Bash, or PowerShell scripts from S3. It automatically handles Python virtual environments, dependency installation from `requirements.txt` files, and entrypoint detection for projects stored in folders or archives.
- **`run-presigned-rc.sh`**: A simpler version of the script runner, focused on fetching a Python script and its associated `Requirements.txt` file from an S3 bucket.

Both runners verify object ownership with `--expected-owner`/`EXPECTED_OWNER` before generating a pre-signed URL. This keeps the workflow compatible with the AWS CLI (which does not accept `--expected-bucket-owner` on `aws s3 presign`) while still preventing cross-account spoofing when you supply the expected AWS Account ID.

**Usage (`run-presigned-canary.sh`):**

```bash
# Execute a python script from an S3 bucket
./run-presigned-canary.sh --bucket "my-script-bucket" --script "scripts/my-script.py" --region "ap-southeast-1"
```

### 3\. ENA Driver Utilities

These scripts help manage the AWS Elastic Network Adapter (ENA) for enhanced networking on EC2 instances.

- **`ena-install.sh`**: Installs or updates the ENA driver from the official `amzn-drivers` GitHub repository. It handles kernel module backup, compilation, installation, and bootloader configuration. It also provides a rollback option.
- **`ena-express-config.sh`**: Applies persistent network performance optimizations specifically for ENA Express, which is designed to improve single-flow latency. The script can set MTU, configure TCP buffer limits, and disable Large LLQ. It also includes a rollback option.

**Usage:**

```bash
# Install the latest ENA driver
sudo ./ena-install.sh --apply

# Apply ENA Express optimizations
sudo ./ena-express-config.sh --apply

# Revert ENA Express optimizations
sudo ./ena-express-config.sh --revert
```

### 4\. Dependencies

- **`Dependencies/Requirements.txt`**: A text file containing the Python packages required by the `AWS-list-resources-all-*.py` scripts. These packages include `boto3` together with a matching `botocore` pin, `openpyxl`, `defusedxml`, `cron_descriptor`, `tzlocal`, and `tzdata`. Optional entries for handling S3 Inventory and Storage Lens manifests compressed with Zstandard, LZ4, Brotli, or Snappy are also provided (`zstandard`, `lz4`, `brotli`, and `python-snappy`).

The inventory scripts track the October 2025 region catalogue, including the latest ISO and ISOB west/south partitions, so timezone conversions and pricing lookups stay accurate even in newly launched regions.
