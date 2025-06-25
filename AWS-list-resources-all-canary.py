#!/usr/bin/env python3
"""
AWS Inventory Exporter

Collect metadata from multiple AWS services across one or more accounts
and write the results into an Excel workbook.
"""

# ───────────────────────────── imports ──────────────────────────────
from __future__ import annotations

import argparse
import json
import logging
import multiprocessing
import os
import random
import re
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from itertools import islice
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)
from cron_descriptor import get_description as _cron_desc
from defusedxml import ElementTree as ET
from openpyxl import Workbook
from openpyxl.utils.cell import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.worksheet.worksheet import Worksheet
from tzlocal import get_localzone
from xml.sax.saxutils import unescape
from zoneinfo import ZoneInfo

# ─────────────────────────── configuration ──────────────────────────
CONNECT_TIMEOUT = int(os.getenv("AWS_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = int(os.getenv("AWS_READ_TIMEOUT", "60"))
MAX_ATTEMPTS = int(os.getenv("AWS_MAX_ATTEMPTS", "8"))

# -- parallelism -----------------------------------------------------
CPU_COUNT = multiprocessing.cpu_count()
MAX_REGIONS_IN_FLIGHT = min(int(os.getenv("MAX_PAR_REGION", "3")), 8)
MAX_TASKS_IN_REGION = min(int(os.getenv("MAX_PAR_TASK", "4")), 8)
_POOL_SIZE = MAX_REGIONS_IN_FLIGHT * MAX_TASKS_IN_REGION + 5  # HTTP-pool

RETRY_CONFIG = Config(
    retries={
        "mode": os.getenv("AWS_RETRY_MODE", "standard"),
        "max_attempts": MAX_ATTEMPTS,
    },
    connect_timeout=CONNECT_TIMEOUT,
    read_timeout=READ_TIMEOUT,
    max_pool_connections=_POOL_SIZE,
)

EXCEL_FILENAME = "aws_inventory.xlsx"
_MAX_EXCEL_NAME_LEN = 31

# cache-size defaults (env-overridable)
CLIENT_CACHE_MAX = int(os.getenv("AWS_CLIENT_CACHE_MAX", "128"))
FETCH_SPECS_CACHE_MAX = int(os.getenv("AWS_FETCH_SPECS_CACHE_MAX", "256"))
PARSE_VPN_CACHE_MAX = int(os.getenv("AWS_PARSE_VPN_CACHE_MAX", "128"))
BUCKET_REGION_CACHE_MAX = int(os.getenv("AWS_BUCKET_REGION_CACHE_MAX", "128"))
STORAGE_LENS_CACHE_MAX = int(os.getenv("AWS_STORAGE_LENS_CACHE_MAX", "128"))

# S3 / CloudWatch constants
_METRIC_PERIOD = 86_400  # seconds per day
_METRIC_OFFSET = 48  # hours of latency to tolerate
MAX_KEYS_FOR_FULL_SCAN = int(os.getenv("AWS_MAX_KEYS_FOR_FULL_SCAN", "20000"))

# misc
SECS_PER_YEAR = 31_536_000
DEFAULT_ROLE = "OrganizationAccountAccessRole"

REGION_TZ: Dict[str, str] = {
    "us-east-1": "America/New_York",
    "us-east-2": "America/Chicago",
    "us-west-1": "America/Los_Angeles",
    "us-west-2": "America/Los_Angeles",
    "ca-central-1": "America/Toronto",
    "sa-east-1": "America/Sao_Paulo",
    "eu-west-1": "Europe/Dublin",
    "eu-west-2": "Europe/London",
    "eu-west-3": "Europe/Paris",
    "eu-north-1": "Europe/Stockholm",
    "eu-central-1": "Europe/Berlin",
    "eu-central-2": "Europe/Zurich",
    "eu-south-1": "Europe/Rome",
    "ap-south-1": "Asia/Kolkata",
    "ap-northeast-1": "Asia/Tokyo",
    "ap-northeast-2": "Asia/Seoul",
    "ap-northeast-3": "Asia/Tokyo",
    "ap-southeast-1": "Asia/Singapore",
    "ap-southeast-2": "Australia/Sydney",
    "ap-southeast-3": "Asia/Jakarta",
    "ap-east-1": "Asia/Hong_Kong",
    "me-south-1": "Asia/Bahrain",
    "me-central-1": "Asia/Dubai",
    "af-south-1": "Africa/Johannesburg",
    "af-north-1": "Africa/Cairo",
}

# ───────────────────────────── logging ──────────────────────────────
LOG_FMT = "%(asctime)s %(levelname)-7s [%(account)s] %(name)s:%(lineno)d — %(message)s"
DATE_FMT = "%Y-%m-%dT%H:%M:%S%z"


class _TZFormatter(logging.Formatter):
    """Format %(asctime)s in the region’s local time-zone."""

    def __init__(self, fmt: str, datefmt: str):
        super().__init__(fmt, datefmt)
        region = (
            os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )
        self._tz = ZoneInfo(REGION_TZ.get(region, "UTC"))

    def formatTime(
        self, record: logging.LogRecord, datefmt: Optional[str] = None
    ) -> str:  # noqa: N802
        dt = datetime.fromtimestamp(record.created, tz=self._tz)
        return dt.strftime(datefmt or self.datefmt or "%Y-%m-%d %H:%M:%S%z")


class _AccountFilter(logging.Filter):
    """Guarantee `.account` exists on every log record."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        record.account = getattr(record, "account", "-")
        return True


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_TZFormatter(LOG_FMT, DATE_FMT))

logger = logging.getLogger("aws_inventory")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
logger.addHandler(_handler)
logger.addFilter(_AccountFilter())
logger.propagate = False


def log(level: str, msg: str, *args, account: str = "-", **kwargs) -> None:
    """Shortcut that injects `extra={'account': …}`."""
    extra = kwargs.pop("extra", {})
    extra.setdefault("account", account)
    getattr(logger, level)(msg, *args, extra=extra, **kwargs)


# ───────────────────────── helper utilities ─────────────────────────


def chunked(iterable, n: int):  # noqa: ANN001
    """Yield successive *n*-sized chunks from *iterable*."""
    it = iter(iterable)
    while piece := list(islice(it, n)):
        yield piece


_thread_local = threading.local()
_client_eviction_lock = threading.Lock()  # multi-thread cache eviction


def _lru_cache_per_thread() -> OrderedDict:  # noqa: D401
    if not hasattr(_thread_local, "client_cache"):
        _thread_local.client_cache = OrderedDict()
    return _thread_local.client_cache  # type: ignore[return-value]


def aws_client(
    service: str,
    region: str,
    session: Optional[boto3.Session] = None,
) -> BaseClient:
    """
    Thread-local LRU cache around ``boto3.Session.client``.
    boto3 clients are **not** thread-safe; building them on demand and
    caching per-thread avoids lock-contention in the AWS SDK.
    """
    if session is None:
        if not hasattr(_thread_local, "default_session"):
            _thread_local.default_session = boto3.Session()
        session = _thread_local.default_session  # type: ignore[assignment]

    cache: OrderedDict = _lru_cache_per_thread()
    key = (id(session), service, region)
    if key in cache:
        cache.move_to_end(key)
        return cache[key]

    client = session.client(service, region_name=region, config=RETRY_CONFIG)
    cache[key] = client

    # bounded cache
    if len(cache) > CLIENT_CACHE_MAX:
        with _client_eviction_lock:
            if len(cache) > CLIENT_CACHE_MAX:
                cache.popitem(last=False)

    return client


def retry_with_backoff(
    fn: Callable[..., Dict[str, Any]],
    *,
    max_attempts: int = MAX_ATTEMPTS,
    base_delay: float = 1.0,
    max_delay: float = 20.0,
    default: Optional[Dict[str, Any]] = None,
    account: str = "-",
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Retry throttled AWS calls with exponential back-off (+30 % jitter).
    """
    throttling_codes = {
        "Throttling",
        "ThrottlingException",
        "RequestLimitExceeded",
        "TooManyRequestsException",
        "SlowDown",
        "ProvisionedThroughputExceededException",
    }

    for attempt in range(1, max_attempts + 1):
        try:
            return fn(**kwargs)

        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in throttling_codes and not code.startswith("Throttling"):
                raise  # not a throttle

            if attempt == max_attempts:
                log(
                    "error",
                    f"Max retries reached for {getattr(fn,'__name__',fn)}: {code}",
                    account=account,
                )
                return default or {}

            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            delay += delay * 0.3 * random.random()  # ±30 % jitter (AWS guidance)
            log(
                "warning",
                f"Throttled ({code}) {attempt}/{max_attempts}; retrying in "
                f"{delay:.1f}s",
                account=account,
            )
            time.sleep(delay)

    return default or {}  # never reached – type guard


def _safe_aws_call(
    fn: Callable[..., Dict[str, Any]],
    *,
    default: Optional[Dict[str, Any]] = None,
    account: str = "-",
    retry: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Run an AWS SDK call and swallow *known-benign* faults.
    """
    kwargs.pop("service", None)
    kwargs.pop("operation", None)

    benign_s3_errors = {
        "NoSuchLifecycleConfiguration",
        "NoSuchTagSet",
        "NoSuchBucketPolicy",
        "NoSuchPublicAccessBlockConfiguration",
    }

    try:
        return (
            retry_with_backoff(fn, account=account, **kwargs) if retry else fn(**kwargs)
        )

    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        client = getattr(fn, "__self__", None)
        is_s3 = (
            isinstance(client, BaseClient)
            and client.meta.service_model.service_name == "s3"
        )
        if is_s3 and code in benign_s3_errors:
            return default or {}

        log(
            "warning",
            f"AWS call {getattr(fn, '__name__', fn)} failed: {exc}",
            account=account,
        )
        return default or {}

    except EndpointConnectionError as exc:
        log("warning", f"Endpoint error: {exc}", account=account)
        return default or {}


def _safe_paginator(
    paginate_fn: Callable[..., Any],
    *,
    account: str = "-",
    **kwargs: Any,
) -> Iterator[Dict[str, Any]]:
    """
    Yield pages from a Boto3 paginator, swallowing network / client faults.
    """
    try:
        yield from paginate_fn(**kwargs)  # iterable → iterator (yield-from)
    except (ClientError, EndpointConnectionError) as exc:
        log("warning", f"Paginator aborted: {exc}", account=account)


# ─────────────────────── time / formatting helpers ───────────────────────

_TIMEZONE_OFFSET_RE = re.compile(r"([+-]\d{2})(\d{2})$")  # +0800 → +08:00


def _tz_for(region: str) -> str:
    if region in REGION_TZ:
        return REGION_TZ[region]
    if region.startswith(("us-gov-", "us-iso-")):
        return "America/Denver" if region.endswith("-iso-b-1") else "America/New_York"
    if region.startswith("cn-"):
        return "Asia/Shanghai"
    if region.startswith("il-"):
        return "Asia/Jerusalem"
    return "UTC"


def to_local(dt: Union[str, datetime, None], region: str) -> str:
    if not dt:
        return ""
    if isinstance(dt, str):
        iso = _TIMEZONE_OFFSET_RE.sub(r"\1:\2", dt.strip().replace("Z", "+00:00"))
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError:
            return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    loc = dt.astimezone(ZoneInfo(_tz_for(region)))
    return f"{loc:%Y-%m-%d %H:%M:%S} {_tz_for(region)}"


def human_size(size_bytes: Optional[int]) -> str:
    if size_bytes is None:
        return ""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{int(size)} B"
    return f"{size:.1f}".rstrip("0").rstrip(".") + f" {unit}"


def humanise_schedule(expr: str) -> Tuple[str, str]:
    if expr.startswith("rate("):
        val, unit = expr[5:-1].split()
        unit = unit.rstrip("s")
        return (f"Every {unit}" if val == "1" else f"Every {val} {unit}s"), ""
    if expr.startswith("at("):
        return "One-time", expr[3:-1]
    if expr.startswith("cron("):
        parts = expr[5:-1].split()
        if len(parts) == 6:
            m, h, dom, mon, dow, _ = parts
            if dom == "?" and mon == "*" and dow in ("?", "*"):
                return f"Daily at {h.zfill(2)}:{m.zfill(2)}", ""
            if dom.isdigit() and mon == "*" and dow in ("?", "*"):
                return f"Monthly on day {dom} at {h.zfill(2)}:{m.zfill(2)}", ""
        cron_expr = expr[5:-1]
        try:
            return _cron_desc(cron_expr, locale="en"), ""
        except TypeError:
            return _cron_desc(cron_expr), ""
    return expr, ""


def seconds_to_years(sec: Union[int, float, None]) -> str:
    if sec is None or sec < 0:
        return ""
    yrs = sec / SECS_PER_YEAR
    if yrs.is_integer():
        yrs_int = int(yrs)
        return f"{yrs_int} Year" + ("" if yrs_int == 1 else "s")
    return f"{yrs:.1f} Years"


def _plan_term_seconds(plan: dict) -> Optional[int]:
    if "termDurationInSeconds" in plan:
        return plan["termDurationInSeconds"]
    start = plan.get("startTime") or plan.get("start")
    end = plan.get("endTime") or plan.get("end")
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace("Z", "+00:00"))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace("Z", "+00:00"))
    if start and end:
        return int((end - start).total_seconds())
    return None


# ─────────────────────────────────────────────────────────────────────
# IAM ROLE ASSUMPTION
# ─────────────────────────────────────────────────────────────────────


def assume_role(
    account_id: str,
    role_name: str = DEFAULT_ROLE,
    region: str = "us-east-1",
) -> Optional[boto3.Session]:
    sts = boto3.client("sts", region_name=region, config=RETRY_CONFIG)

    def _assume() -> Dict[str, str]:
        return sts.assume_role(
            RoleArn=f"arn:aws:iam::{account_id}:role/{role_name}",
            RoleSessionName="InventorySession",
        )["Credentials"]

    try:
        creds: Dict[str, str] = {}
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                creds = _assume()
                break
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if (
                    code in {"AccessDenied", "AccessDeniedException"}
                    or attempt == MAX_ATTEMPTS
                ):
                    raise
                time.sleep(min(2**attempt, 20))
        if not creds:
            return None
    except (ClientError, NoCredentialsError) as exc:
        logger.error("AssumeRole failed: %s", exc, extra={"account": account_id})
        return None

    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )


# ─────────────────────────────────────────────────────────────────────
# BOUNDED CACHES
# ─────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=FETCH_SPECS_CACHE_MAX)
def fetch_instance_type_specs(
    types: tuple[str, ...],
    region: str,
    session: boto3.Session,
) -> dict[str, Any]:
    """
    Return {instance_type: {vCPUs, Memory, NetworkPerformance}} for every
    *valid* type in *types*.

    • Works in ≤ 100-ID batches (API limit).
    • If the batch contains unknown / retired types, it removes them,
      logs a warning, and retries the remaining IDs - preventing
      `InvalidInstanceType` from killing the whole EC2 scan.
    """
    ec2 = aws_client("ec2", region, session)
    out: dict[str, Any] = {}

    # we may need to mutate the set, so copy into a list
    pending: list[str] = list(types)

    while pending:
        chunk = pending[:100]
        try:
            resp = ec2.describe_instance_types(InstanceTypes=chunk)
            for it in resp["InstanceTypes"]:
                mib = it["MemoryInfo"]["SizeInMiB"]
                gib = mib / 1024
                out[it["InstanceType"]] = {
                    "vCPUs": it["VCpuInfo"]["DefaultVCpus"],
                    "Memory": (
                        f"{gib/1024:.1f} TB" if gib >= 1024 else f"{round(gib)} GB"
                    ),
                    "NetworkPerformance": it["NetworkInfo"].get(
                        "NetworkPerformance", ""
                    ),
                }

            pending = pending[100:]  # finished this batch; move on

        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code != "InvalidInstanceType":
                raise  # real error – re-raise

            # Parse “… do not exist: [g3.4xlarge, r3.8xlarge]”
            msg = exc.response["Error"]["Message"]
            bad = [t.strip() for t in msg.split(":")[-1].strip(" []").split(",")]
            log(
                "warning",
                f"Region {region}: unknown / retired instance types skipped: {', '.join(bad)}",
                account="-",
            )
            # remove bad IDs from *pending* and retry the reduced batch
            pending = [t for t in pending if t not in bad]

    return out


@lru_cache(maxsize=PARSE_VPN_CACHE_MAX)
def parse_vpn_config(xml: str) -> Tuple[str, List[str]]:
    root = ET.fromstring(unescape(xml).encode())
    cg = root.findtext(".//customer_gateway/tunnel_outside_address/ip_address") or "N/A"
    ips = {
        ip.text
        for ip in root.findall(
            ".//ipsec_tunnel/vpn_gateway/tunnel_outside_address/ip_address"
        )
        if ip.text
    }
    return cg, sorted(ips) or ["N/A"]


@lru_cache(maxsize=BUCKET_REGION_CACHE_MAX)
def bucket_region(
    bucket: str,
    region_hint: str,
    session: Optional[boto3.Session] = None,
) -> Optional[str]:
    s3 = aws_client("s3", region_hint, session)
    try:
        loc = s3.get_bucket_location(Bucket=bucket)["LocationConstraint"]
        if loc is None:
            if region_hint.startswith("us-gov"):
                return "us-gov-west-1"
            if region_hint.startswith(("us-iso-", "us-isob-")):
                return "us-iso-east-1"
            if region_hint.startswith("cn-"):
                return "cn-north-1"
            return "us-east-1"
        return loc
    except s3.exceptions.NoSuchBucket:
        logger.warning("Bucket %s no longer exists", bucket)
    except s3.exceptions.ClientError as exc:
        if exc.response["Error"].get("Code") == "PermanentRedirect":
            return exc.response["Error"]["BucketRegion"]
        logger.debug("bucket_region(%s) failed: %s", bucket, exc)
    return None


@lru_cache(maxsize=STORAGE_LENS_CACHE_MAX)
def storage_lens_metrics(
    bucket: str,
    region: str,
    session: Optional[boto3.Session] = None,
) -> Tuple[Optional[int], Optional[int]]:
    try:
        sl = aws_client("s3control", region, session)
        acct = aws_client("sts", region, session).get_caller_identity()["Account"]
        cfgs = sl.list_storage_lens_configurations(AccountId=acct).get(
            "StorageLensConfigurationList", []
        )
        if not cfgs:
            return None, None
        cfg = sl.get_storage_lens_configuration(
            AccountId=acct,
            ConfigId=cfgs[0]["Id"],
        )["StorageLensConfiguration"]
        metrics = cfg["AccountLevel"]["BucketLevel"]["AdvancedMetrics"]
        return metrics.get("TotalStorageBytes"), metrics.get("TotalObjectCount")
    except Exception as exc:
        logger.debug("StorageLens lookup failed for %s: %s", bucket, exc)
        return None, None


# --------------------------------------------------------------------------
# VPN COLLECTOR
# --------------------------------------------------------------------------


def get_vpn_details(ec2_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    resp = _safe_aws_call(
        ec2_client.describe_vpn_connections,
        default={"VpnConnections": []},
        account=alias,
    )
    for conn in resp.get("VpnConnections", []):
        xml = conn.get("CustomerGatewayConfiguration", "")
        cg, tns = parse_vpn_config(xml) if xml else ("N/A", ["N/A"])
        name = next(
            (t["Value"] for t in conn.get("Tags", []) if t["Key"].lower() == "name"),
            "",
        )
        out.append(
            {
                "VpnConnectionId": conn.get("VpnConnectionId", ""),
                "Name": name,
                "State": conn.get("State", ""),
                "CustomerGateway": conn.get("CustomerGatewayId", ""),
                "CustomerGatewaySource": cg,
                "TunnelOutsideIps": ", ".join(tns),
                "Region": ec2_client.meta.region_name,
                "AccountAlias": alias,
            }
        )
    return out


# --------------------------------------------------------------------------
# S3 HELPERS & COLLECTOR
# --------------------------------------------------------------------------


def _s3_select_sum_and_count(
    s3: BaseClient,
    bucket: str,
    key: str,
    *,
    account: str = "-",
) -> tuple[int, int]:
    sql = (
        'SELECT CAST(sum(cast(s."Size" AS int)) AS bigint), '
        "       CAST(count(1) AS bigint) "
        "FROM S3Object s"
    )
    resp = _safe_aws_call(
        s3.select_object_content,
        account=account,
        retry=True,
        Bucket=bucket,
        Key=key,
        ExpressionType="SQL",
        Expression=sql,
        InputSerialization={
            "CSV": {"FileHeaderInfo": "USE"},
            "CompressionType": "NONE",
        },
        OutputSerialization={"JSON": {"RecordDelimiter": "\n"}},
    )
    buf = b""
    for ev in resp.get("Payload", []):
        if rec := ev.get("Records"):
            buf += rec["Payload"]
    return tuple(json.loads(buf.decode()) if buf else (0, 0))


@lru_cache(maxsize=STORAGE_LENS_CACHE_MAX)
def inventory_metrics(  # noqa: C901
    bucket: str,
    region_hint: str,
    session: Optional[boto3.Session] = None,
) -> tuple[Optional[int], Optional[int]]:
    try:
        inv = aws_client("s3", region_hint, session)
        cfgs = inv.list_bucket_inventory_configurations(Bucket=bucket).get(
            "InventoryConfigurationList", []
        )
        if not cfgs:
            return None, None
        cfg = cfgs[0]
        inv_id = cfg["Id"]
        prefix = cfg["Destination"]["S3BucketDestination"].get("Prefix", "").rstrip("/")
        date_part = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        manifest_key = f"{prefix or inv_id}/{date_part}/manifest.json"
        dest = cfg["Destination"]["S3BucketDestination"]["Bucket"].split(":::")[-1]
        dest_region = bucket_region(dest, region_hint, session)
        s3_dest = aws_client("s3", dest_region, session)
        manifest = json.loads(
            s3_dest.get_object(Bucket=dest, Key=manifest_key)["Body"].read()
        )
        total_bytes = total_objects = 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [
                pool.submit(
                    _s3_select_sum_and_count,
                    s3_dest,
                    dest,
                    file_["key"],
                    account=bucket,
                )
                for file_ in manifest["files"]
            ]
            for fut in as_completed(futures):
                b, o = fut.result()
                total_bytes += b
                total_objects += o
        return total_bytes, total_objects
    except Exception as exc:
        logger.warning("Inventory metrics (S3 Select) failed for %s: %s", bucket, exc)
        return None, None


def get_s3_details(
    s3_client: BaseClient,
    all_buckets: List[Dict[str, Any]],
    alias: str,
    region: str,
    session: boto3.Session,
) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=_METRIC_OFFSET + 2)

    buckets = [
        b for b in all_buckets if bucket_region(b["Name"], region, session) == region
    ]
    if not buckets:
        return []

    cw = aws_client("cloudwatch", region, session)
    queries, id_map = [], {}
    for idx, b in enumerate(buckets):
        name = b["Name"]
        for metric, stype, key in [
            ("BucketSizeBytes", "StandardStorage", "size"),
            ("NumberOfObjects", "AllStorageTypes", "count"),
        ]:
            qid = f"b{idx:05}{key[0]}"
            id_map[qid] = (name, key)
            queries.append(
                {
                    "Id": qid,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/S3",
                            "MetricName": metric,
                            "Dimensions": [
                                {"Name": "BucketName", "Value": name},
                                {"Name": "StorageType", "Value": stype},
                            ],
                        },
                        "Period": _METRIC_PERIOD,
                        "Stat": "Average",
                    },
                    "ReturnData": True,
                }
            )

    metric_results: List[Dict[str, Any]] = []
    for chunk in (queries[i : i + 500] for i in range(0, len(queries), 500)):
        token = None
        while True:
            resp = _safe_aws_call(
                cw.get_metric_data,
                default={},
                account=alias,
                MetricDataQueries=chunk,
                StartTime=start,
                EndTime=now,
                ScanBy="TimestampDescending",
                **({"NextToken": token} if token else {}),
            )
            if not resp:
                break
            metric_results.extend(resp.get("MetricDataResults", []))
            token = resp.get("NextToken")
            if not token:
                break

    cw_data = {b["Name"]: {"size": None, "count": None} for b in buckets}
    for r in metric_results:
        vals = r.get("Values")
        if vals:
            name, key = id_map[r["Id"]]
            cw_data[name][key] = int(vals[0])

    rows: List[Dict[str, Any]] = []
    for b in buckets:
        name = b["Name"]
        created = b["CreationDate"]
        size = cw_data[name]["size"]
        count = cw_data[name]["count"]

        method = None
        if size is not None and count is not None:
            method = "CloudWatch"
        elif size is not None:
            count = 0
            method = "CloudWatch-SizeOnly"
        elif count is not None:
            size = 0
            method = "CloudWatch-CountOnly"
        else:
            size, count = storage_lens_metrics(name, region, session)
            if size is not None or count is not None:
                method = "StorageLens"
            else:
                size, count = inventory_metrics(name, region, session)
                if size is not None or count is not None:
                    method = "Inventory"

        if method is None:
            size = count = 0
            for i, page in enumerate(
                s3_client.get_paginator("list_objects_v2").paginate(
                    Bucket=name, PaginationConfig={"PageSize": 250}
                ),
                start=1,
            ):
                for obj in page.get("Contents", []):
                    size += obj.get("Size", 0)
                    count += 1
                    if count >= MAX_KEYS_FOR_FULL_SCAN:
                        size = count = None
                        break
                if count is None or i >= 5:
                    method = "TooLargeToScan" if count is None else "ListObjects"
                    break
            if method is None:
                method = "ListObjects"

        ver = _safe_aws_call(
            s3_client.get_bucket_versioning, default={}, account=alias, Bucket=name
        ).get("Status", "Disabled")

        enc_cfg = _safe_aws_call(
            s3_client.get_bucket_encryption, default={}, account=alias, Bucket=name
        )
        enc = (
            "Enabled"
            if "ServerSideEncryptionConfiguration" in enc_cfg
            else "Not enabled"
        )

        try:
            pab_fn = s3_client.get_public_access_block
        except AttributeError:  # pre-GA preview (rare)
            pab_fn = s3_client.get_bucket_public_access_block

        pab_cfg = _safe_aws_call(pab_fn, default={}, account=alias, Bucket=name).get(
            "PublicAccessBlockConfiguration", {}
        )
        public = "Blocked" if pab_cfg and all(pab_cfg.values()) else "Not fully blocked"

        policy_flag = _safe_aws_call(
            s3_client.get_bucket_policy_status,
            default={"PolicyStatus": {"IsPublic": False}},
            account=alias,
            Bucket=name,
        )["PolicyStatus"].get("IsPublic", False)
        policy = "Public" if policy_flag else "Not Public"

        lifecycle = _safe_aws_call(
            s3_client.get_bucket_lifecycle_configuration,
            default={"Rules": []},
            account=alias,
            Bucket=name,
        ).get("Rules", [])

        tagset = _safe_aws_call(
            s3_client.get_bucket_tagging,
            default={"TagSet": []},
            account=alias,
            Bucket=name,
        )["TagSet"]
        tags = {t["Key"]: t["Value"] for t in tagset}

        rows.append(
            {
                "BucketName": name,
                "CreationDate": to_local(created, region),
                "Region": region,
                "Size": human_size(size),
                "ObjectCount": "" if count is None else count,
                "LastMetricsUpdate": (
                    "" if (size is None and count is None) else to_local(now, region)
                ),
                "MetricsCalculationMethod": method,
                "Versioning": ver,
                "Encryption": enc,
                "PublicAccess": public,
                "PolicyStatus": policy,
                "LifecycleRules": lifecycle,
                "Tags": tags,
                "AccountAlias": alias,
            }
        )
    return rows


# --------------------------------------------------------------------------
# EC2 COLLECTOR
# --------------------------------------------------------------------------
def get_ec2_details(
    ec2_client: BaseClient,
    backup_client: BaseClient,
    alias: str,
    session: boto3.Session,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    instances = [
        inst
        for page in _safe_paginator(
            ec2_client.get_paginator("describe_instances").paginate, account=alias
        )
        for r in page.get("Reservations", [])
        for inst in r.get("Instances", [])
    ]
    if not instances:
        return out

    inst_ids = {i["InstanceId"] for i in instances}
    volume_info: Dict[str, Dict[str, Any]] = {}
    for chunk in chunked(sorted(inst_ids), 500):
        resp = _safe_aws_call(
            ec2_client.describe_volumes,
            default={"Volumes": []},
            account=alias,
            Filters=[{"Name": "attachment.instance-id", "Values": list(chunk)}],
        )
        for v in resp.get("Volumes", []):
            volume_info[v["VolumeId"]] = {
                "Size": v.get("Size"),
                "Type": v.get("VolumeType"),
            }

    types = {i["InstanceType"] for i in instances}
    specs: Dict[str, Any] = {}
    for chunk in chunked(tuple(types), 200):
        specs.update(
            fetch_instance_type_specs(
                tuple(chunk), ec2_client.meta.region_name, session
            )
        )

    eip_resp = _safe_aws_call(
        ec2_client.describe_addresses,
        default={"Addresses": []},
        account=alias,
    )
    eips = {
        addr.get("PublicIp")
        for addr in eip_resp.get("Addresses", [])
        if addr.get("PublicIp")
    }

    prot_i, prot_v, plan_i, plan_v = set(), set(), set(), set()
    all_i = all_v = False
    for page in _safe_paginator(
        backup_client.get_paginator("list_protected_resources").paginate, account=alias
    ):
        for r in page.get("Results", []):
            arn = r.get("ResourceArn", "")
            if arn.endswith("/instance/*"):
                all_i = True
            elif arn.endswith("/volume/*"):
                all_v = True
            elif ":instance/" in arn:
                prot_i.add(arn.rsplit("/", 1)[-1])
            elif ":volume/" in arn:
                prot_v.add(arn.rsplit("/", 1)[-1])

    plans = _safe_aws_call(
        backup_client.list_backup_plans, default={"BackupPlansList": []}, account=alias
    )["BackupPlansList"]
    for p in plans:
        for sel in _safe_aws_call(
            backup_client.list_backup_selections,
            default={"BackupSelectionsList": []},
            account=alias,
            BackupPlanId=p["BackupPlanId"],
        )["BackupSelectionsList"]:
            cfg = _safe_aws_call(
                backup_client.get_backup_selection,
                default={"BackupSelection": {}},
                account=alias,
                BackupPlanId=p["BackupPlanId"],
                SelectionId=sel["SelectionId"],
            )["BackupSelection"]
            for arn in cfg.get("Resources", []):
                if arn.endswith("/instance/*"):
                    all_i = True
                elif arn.endswith("/volume/*"):
                    all_v = True
                elif ":instance/" in arn:
                    plan_i.add(arn.rsplit("/", 1)[-1])
                elif ":volume/" in arn:
                    plan_v.add(arn.rsplit("/", 1)[-1])

    for inst in instances:
        iid = inst["InstanceId"]
        attached = [
            bd["Ebs"]["VolumeId"]
            for bd in inst.get("BlockDeviceMappings", [])
            if bd.get("Ebs", {}).get("VolumeId")
        ]
        covered = (
            all_i
            or iid in prot_i
            or iid in plan_i
            or any(v in prot_v or v in plan_v for v in attached)
            or (all_v and attached)
        )
        spec = specs.get(inst["InstanceType"], {})
        pub = inst.get("PublicIpAddress", "")
        ipt = "Elastic" if pub in eips else ("Ephemeral" if pub else "")
        out.append(
            {
                "Name": next(
                    (
                        t["Value"]
                        for t in inst.get("Tags", [])
                        if t["Key"].lower() == "name"
                    ),
                    "",
                ),
                "InstanceId": iid,
                "InstanceType": inst["InstanceType"],
                "KeyPair": inst.get("KeyName", ""),
                "vCPUs": spec.get("vCPUs"),
                "Memory": spec.get("Memory"),
                "NetworkPerformance": spec.get("NetworkPerformance"),
                "AvailabilityZone": inst.get("Placement", {}).get(
                    "AvailabilityZone", ""
                ),
                "PublicIP": pub,
                "PrivateIP": inst.get("PrivateIpAddress", ""),
                "IPType": ipt,
                "EBSVolumes": ", ".join(
                    f"{v}:{volume_info.get(v,{}).get('Size','?')}GB" for v in attached
                ),
                "OS": inst.get("PlatformDetails", "Linux/UNIX"),
                "AWSBackup": "Covered" if covered else "Not Covered",
                "Region": ec2_client.meta.region_name,
                "AccountAlias": alias,
            }
        )
    return out


# ------------------------------------------------------------------
# EC2 RESERVED-INSTANCES COLLECTOR
# ------------------------------------------------------------------
def get_ec2_reserved_instances(
    ec2_client: BaseClient,
    alias: str,
    _session: boto3.Session,
) -> list[dict[str, Any]]:
    """
    Return every active EC2 Reserved Instance in *ec2_client.region*.

    Note: AWS now recommends **regional** RIs, so AvailabilityZone is
    usually blank – we omit that column entirely to avoid empty cells.
    """
    out: list[dict[str, Any]] = []

    # `describe_reserved_instances` isn’t paginated, so a single call is fine
    resp = _safe_aws_call(
        ec2_client.describe_reserved_instances,
        default={"ReservedInstances": []},
        account=alias,
        Filters=[{"Name": "state", "Values": ["active"]}],
    )

    for ri in resp.get("ReservedInstances", []):
        out.append(
            {
                "ReservedInstancesId": ri["ReservedInstancesId"],
                "InstanceType": ri["InstanceType"],
                "Scope": ri.get("Scope", ""),  # Region | AvailabilityZone
                "InstanceCount": ri.get("InstanceCount", 0),
                "OfferingType": ri.get("OfferingType", ""),
                "ProductDescription": ri.get("ProductDescription", ""),
                "Duration": seconds_to_years(ri["Duration"]),
                "FixedPrice": ri["FixedPrice"],
                "UsagePrice": ri["UsagePrice"],
                "CurrencyCode": ri["CurrencyCode"],
                "StartTime": to_local(ri.get("Start"), ec2_client.meta.region_name),
                "State": ri["State"],
                "Region": ec2_client.meta.region_name,
                "AccountAlias": alias,
            }
        )

    return out


# ------------------------------------------------------------------
# SAVINGS-PLANS COLLECTOR
# ------------------------------------------------------------------


def get_savings_plan_details(
    sp_client: BaseClient, alias: str, _session: boto3.Session
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    while True:
        params: Dict[str, Any] = {"nextToken": next_token} if next_token else {}
        resp = _safe_aws_call(
            sp_client.describe_savings_plans, default={}, account=alias, **params
        )
        if not resp:
            break
        for plan in resp.get("savingsPlans", []):
            start = plan.get("startTime") or plan.get("start")
            end = plan.get("endTime") or plan.get("end")
            out.append(
                {
                    "SavingsPlanId": plan.get("savingsPlanId", ""),
                    "SavingsPlanArn": plan.get("savingsPlanArn", ""),
                    "State": plan.get("state", ""),
                    "Start": to_local(start, sp_client.meta.region_name),
                    "End": to_local(end, sp_client.meta.region_name),
                    "Term": seconds_to_years(_plan_term_seconds(plan)),
                    "PaymentOption": plan.get("paymentOption", ""),
                    "PlanType": plan.get("savingsPlanType", ""),
                    "Region": "global",
                    "AccountAlias": alias,
                }
            )
        next_token = resp.get("nextToken")
        if not next_token:
            break
    return out


# --------------------------------------------------------------------------
# RDS COLLECTORS
# --------------------------------------------------------------------------


def get_rds_details(
    rds_client: BaseClient, alias: str, session: boto3.Session
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    all_dbs: List[Dict[str, Any]] = []
    for page in _safe_paginator(
        rds_client.get_paginator("describe_db_instances").paginate, account=alias
    ):
        all_dbs.extend(page.get("DBInstances", []))
    if not all_dbs:
        return out
    classes = {db["DBInstanceClass"].removeprefix("db.") for db in all_dbs}
    specs: Dict[str, Any] = {}
    for chunk in chunked(sorted(classes), 20):
        specs.update(
            fetch_instance_type_specs(
                tuple(chunk), rds_client.meta.region_name, session
            )
        )
    for db in all_dbs:
        arn = db["DBInstanceArn"]
        tags = {
            t["Key"]: t["Value"]
            for t in _safe_aws_call(
                rds_client.list_tags_for_resource,
                default={"TagList": []},
                account=alias,
                ResourceName=arn,
            )["TagList"]
        }
        cls = db["DBInstanceClass"]
        spec = specs.get(cls.removeprefix("db."), {})
        subnet_grp = db.get("DBSubnetGroup", {}).get("DBSubnetGroupName", "")
        vpc_sg_ids = [
            sg.get("VpcSecurityGroupId", "") for sg in db.get("VpcSecurityGroups", [])
        ]
        pub_bool = db.get("PubliclyAccessible", False)
        visibility = "Public" if pub_bool else "Private"
        out.append(
            {
                "DBInstanceIdentifier": db.get("DBInstanceIdentifier", ""),
                "Engine": db.get("Engine", ""),
                "DBInstanceClass": cls,
                "vCPUs": spec.get("vCPUs"),
                "Memory": spec.get("Memory"),
                "MultiAZ": db.get("MultiAZ", False),
                "PubliclyAccessible": pub_bool,
                "EndpointVisibility": visibility,
                "StorageType": db.get("StorageType", ""),
                "AllocatedStorage": db.get("AllocatedStorage", 0),
                "EndpointAddress": db.get("Endpoint", {}).get("Address", ""),
                "EndpointPort": db.get("Endpoint", {}).get("Port", ""),
                "InstanceCreateTime": to_local(
                    db.get("InstanceCreateTime"), rds_client.meta.region_name
                ),
                "LicenseModel": db.get("LicenseModel", ""),
                "VpcSecurityGroupIds": vpc_sg_ids,
                "DBSubnetGroup": subnet_grp,
                "Tags": tags,
                "Region": rds_client.meta.region_name,
                "AccountAlias": alias,
            }
        )
    return out


def get_rds_reserved_instances(
    rds_client: BaseClient, alias: str, session: boto3.Session
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in _safe_paginator(
        rds_client.get_paginator("describe_reserved_db_instances").paginate,
        account=alias,
    ):
        for ri in page.get("ReservedDBInstances", []):
            out.append(
                {
                    "ReservedDBInstanceId": ri["ReservedDBInstanceId"],
                    "DBInstanceClass": ri["DBInstanceClass"],
                    "Duration": seconds_to_years(ri["Duration"]),
                    "FixedPrice": ri["FixedPrice"],
                    "UsagePrice": ri["UsagePrice"],
                    "CurrencyCode": ri["CurrencyCode"],
                    "StartTime": to_local(
                        ri.get("StartTime"), rds_client.meta.region_name
                    ),
                    "State": ri["State"],
                    "MultiAZ": ri["MultiAZ"],
                    "OfferingType": ri["OfferingType"],
                    "ProductDescription": ri["ProductDescription"],
                    "Region": rds_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


# --------------------------------------------------------------------------
# SES, SNS & ROUTE53 COLLECTORS
# --------------------------------------------------------------------------
def get_ses_details(ses_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in _safe_paginator(
        ses_client.get_paginator("list_identities").paginate, account=alias
    ):
        for ident in page.get("Identities", []):
            attrs = _safe_aws_call(
                ses_client.get_identity_verification_attributes,
                default={"VerificationAttributes": {}},
                account=alias,
                Identities=[ident],
            )["VerificationAttributes"]
            status = attrs.get(ident, {}).get("VerificationStatus", "")
            out.append(
                {
                    "Identity": ident,
                    "VerificationStatus": status,
                    "Region": ses_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


def get_sns_details(sns_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in _safe_paginator(
        sns_client.get_paginator("list_topics").paginate, account=alias
    ):
        for topic in page.get("Topics", []):
            arn = topic["TopicArn"]

            subs: List[str] = []
            for sp in _safe_paginator(
                sns_client.get_paginator("list_subscriptions_by_topic").paginate,
                account=alias,
                TopicArn=arn,
            ):
                subs.extend(
                    f"{s.get('Protocol','')}://{s.get('Endpoint','')}"
                    for s in sp.get("Subscriptions", [])
                )

            out.append(
                {
                    "TopicArn": arn,
                    "TopicName": arn.split(":")[-1],
                    "SubscriptionCount": len(subs),
                    "Subscriptions": subs,  # <- now populated
                    "Region": sns_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


def get_route53_details(r53_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    """
    Return one row per hosted zone that the caller can see.

    *RecordTypes* → unique RR-set types in the zone
    *HealthChecks* → every health-check ID referenced by a record set
    """
    out: List[Dict[str, Any]] = []

    zones = _safe_aws_call(
        r53_client.list_hosted_zones,
        default={"HostedZones": []},
        account=alias,
    )["HostedZones"]

    for z in zones:
        zid = z["Id"].split("/")[-1]

        # ── zone details -------------------------------------------------
        det = _safe_aws_call(
            r53_client.get_hosted_zone,
            default={},
            account=alias,
            Id=zid,
        )
        vpcs = [
            {"VPCRegion": v["VPCRegion"], "VPCId": v["VPCId"]}
            for v in det.get("VPCs", [])
        ]
        ns = det.get("DelegationSet", {}).get("NameServers", [])

        sec = _safe_aws_call(
            r53_client.get_dnssec,
            default={},
            account=alias,
            HostedZoneId=zid,
        ).get("Status", {})
        dnssec = f'{sec.get("Status","")}/{sec.get("ServeSignature","")}'.strip("/")

        tags = {
            t["Key"]: t["Value"]
            for t in _safe_aws_call(
                r53_client.list_tags_for_resource,
                default={"ResourceTagSet": {"Tags": []}},
                account=alias,
                ResourceType="hostedzone",
                ResourceId=zid,
            )["ResourceTagSet"]["Tags"]
        }

        # ── NEW: scan RR-sets once for types & health-checks -------------
        record_types: set[str] = set()
        health_checks: set[str] = set()

        for page in _safe_paginator(
            r53_client.get_paginator("list_resource_record_sets").paginate,
            account=alias,
            HostedZoneId=zid,
        ):
            for rr in page.get("ResourceRecordSets", []):

                record_types.add(rr.get("Type", ""))
                if "HealthCheckId" in rr:
                    health_checks.add(rr["HealthCheckId"])

        out.append(
            {
                "Name": z.get("Name", "").rstrip("."),
                "Id": z["Id"],
                "Config": z.get("Config", {}),
                "ResourceRecordSetCount": z.get("ResourceRecordSetCount", 0),
                "RecordTypes": sorted(record_types),
                "Tags": tags,
                "VPCAssociations": vpcs,
                "HealthChecks": sorted(health_checks),
                "DNSSECStatus": dnssec,
                "DelegationSet": ns,
                "Region": "global",
                "AccountAlias": alias,
            }
        )

    return out


# --------------------------------------------------------------------------
# KMS COLLECTOR
# --------------------------------------------------------------------------
def _collect_key_aliases(kms: BaseClient, key_id: str, alias: str) -> List[str]:
    aliases: List[str] = []
    for page in _safe_paginator(
        kms.get_paginator("list_aliases").paginate, account=alias, KeyId=key_id
    ):
        aliases.extend(
            a["AliasName"]
            for a in page.get("Aliases", [])
            if a.get("AliasName", "").startswith("alias/")
        )
    return aliases


def get_kms_details(kms_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    """
    Return metadata for every KMS key the caller can see in *kms_client.region*.

    • For **AWS-managed keys** (`KeyManager == "AWS"`) we skip the calls that
      always fail (`ListResourceTags`, `GetKeyRotationStatus`, `ListGrants`),
      so no AccessDenied warnings are emitted.
    """
    out: List[Dict[str, Any]] = []

    for page in _safe_paginator(
        kms_client.get_paginator("list_keys").paginate, account=alias
    ):
        for key in page.get("Keys", []):
            key_id = key["KeyId"]

            meta = _safe_aws_call(
                kms_client.describe_key,
                default={},
                account=alias,
                KeyId=key_id,
            ).get("KeyMetadata", {})

            if not meta:
                continue

            aws_managed = meta.get("KeyManager") == "AWS"

            # ── optional look-ups (skip on AWS-managed keys) --------------
            tags = {}  # type: Dict[str, str]
            rotation = False  # type: bool
            grants = 0  # type: int

            if not aws_managed:
                tags = {
                    t["TagKey"]: t["TagValue"]
                    for page in _safe_paginator(
                        kms_client.get_paginator("list_resource_tags").paginate,
                        account=alias,
                        KeyId=key_id,
                    )
                    for t in page.get("Tags", [])
                }

                rotation = _safe_aws_call(
                    kms_client.get_key_rotation_status,
                    default={"KeyRotationEnabled": False},
                    account=alias,
                    KeyId=key_id,
                ).get("KeyRotationEnabled", False)

                grants = sum(
                    len(pg.get("Grants", []))
                    for pg in _safe_paginator(
                        kms_client.get_paginator("list_grants").paginate,
                        account=alias,
                        KeyId=key_id,
                    )
                )

            out.append(
                {
                    "KeyId": key_id,
                    "Description": meta.get("Description", ""),
                    "Enabled": meta.get("Enabled", False),
                    "KeyState": meta.get("KeyState", ""),
                    "KeyManager": meta.get("KeyManager", ""),
                    "KeySpec": meta.get("KeySpec", ""),
                    "KeyUsage": meta.get("KeyUsage", ""),
                    "Origin": meta.get("Origin", ""),
                    "CreationDate": to_local(
                        meta.get("CreationDate"), kms_client.meta.region_name
                    ),
                    "DeletionDate": to_local(
                        meta.get("DeletionDate"), kms_client.meta.region_name
                    ),
                    "ValidTo": to_local(
                        meta.get("ValidTo"), kms_client.meta.region_name
                    ),
                    "MultiRegion": meta.get("MultiRegion", False),
                    "PendingDeletion": meta.get("PendingDeletion", False),
                    "PendingWindowInDays": meta.get("PendingWindowInDays", 0),
                    "AliasNames": _collect_key_aliases(kms_client, key_id, alias),
                    "Tags": tags,
                    "RotationEnabled": rotation,
                    "GrantsCount": grants,
                    "Region": kms_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


# --------------------------------------------------------------------------
# ACM COLLECTOR
# --------------------------------------------------------------------------
def get_acm_details(acm_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in _safe_paginator(
        acm_client.get_paginator("list_certificates").paginate, account=alias
    ):
        for cs in page.get("CertificateSummaryList", []):
            arn = cs["CertificateArn"]
            cert = _safe_aws_call(
                acm_client.describe_certificate,
                default={},
                account=alias,
                CertificateArn=arn,
            ).get("Certificate", {})
            if not cert:
                continue
            out.append(
                {
                    "DomainName": cert.get("DomainName", ""),
                    "CertificateArn": arn,
                    "Status": cert.get("Status", ""),
                    "Type": cert.get("Type", ""),
                    "InUse": bool(cert.get("InUseBy", [])),
                    "Issued": to_local(
                        cert.get("IssuedAt"), acm_client.meta.region_name
                    ),
                    "Expires": to_local(
                        cert.get("NotAfter"), acm_client.meta.region_name
                    ),
                    "RenewalEligibility": cert.get("RenewalEligibility", ""),
                    "SubjectAlternativeNames": cert.get("SubjectAlternativeNames", []),
                    "Region": acm_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


# --------------------------------------------------------------------------
# EventBridge COLLECTOR
# --------------------------------------------------------------------------
def get_eventbridge_details(events: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    buses = _safe_aws_call(
        events.list_event_buses, default={"EventBuses": []}, account=alias
    )["EventBuses"]
    for bus in buses:
        name = bus.get("Name", "")
        for page in _safe_paginator(
            events.get_paginator("list_rules").paginate,
            account=alias,
            EventBusName=name,
        ):
            for rule in page.get("Rules", []):
                expr = rule.get("ScheduleExpression", "")
                if not expr:
                    continue
                freq, det = humanise_schedule(expr)
                targ = _safe_aws_call(
                    events.list_targets_by_rule,
                    default={"Targets": []},
                    account=alias,
                    Rule=rule["Name"],
                    EventBusName=name,
                )["Targets"]
                first = targ[0] if targ else {}
                out.append(
                    {
                        "ScheduleName": rule["Name"],
                        "GroupName": name,
                        "State": rule.get("State", ""),
                        "Expression": expr,
                        "Timezone": _tz_for(events.meta.region_name),
                        "Frequency": freq,
                        "Details": det,
                        "TargetArn": first.get("Arn", ""),
                        "Input": first.get("Input", ""),
                        "Region": events.meta.region_name,
                        "AccountAlias": alias,
                    }
                )
    return out


def get_eventbridge_scheduler_details(
    scheduler: BaseClient, alias: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    groups = {"default"} | {
        g["Name"]
        for page in _safe_paginator(
            scheduler.get_paginator("list_schedule_groups").paginate, account=alias
        )
        for g in page.get("ScheduleGroups", [])
    }
    for grp in groups:
        for page in _safe_paginator(
            scheduler.get_paginator("list_schedules").paginate,
            account=alias,
            GroupName=grp,
        ):
            for s in page.get("Schedules", []):
                name = s.get("Name", "")
                det = _safe_aws_call(
                    scheduler.get_schedule,
                    default={},
                    account=alias,
                    Name=name,
                    GroupName=grp,
                )
                expr = det.get("ScheduleExpression", "")
                freq, desc = humanise_schedule(expr)
                tz = det.get(
                    "ScheduleExpressionTimezone",
                    REGION_TZ.get(scheduler.meta.region_name, "UTC"),
                )
                tgt = det.get("Target", {})
                out.append(
                    {
                        "ScheduleName": name,
                        "GroupName": grp,
                        "State": det.get("State", ""),
                        "Expression": expr,
                        "Timezone": tz,
                        "Frequency": freq,
                        "Details": desc,
                        "TargetArn": tgt.get("Arn", ""),
                        "Input": tgt.get("Input", ""),
                        "Region": scheduler.meta.region_name,
                        "AccountAlias": alias,
                    }
                )
    return out


# --------------------------------------------------------------------------
# AWS Backup COLLECTOR
# --------------------------------------------------------------------------
def get_backup_details(backup_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    plans = _safe_aws_call(
        backup_client.list_backup_plans, default={"BackupPlansList": []}, account=alias
    )["BackupPlansList"]
    for summary in plans:
        pid, vid = summary["BackupPlanId"], summary["VersionId"]
        created = to_local(summary.get("CreationDate"), backup_client.meta.region_name)
        plan = _safe_aws_call(
            backup_client.get_backup_plan,
            default={"BackupPlan": {}},
            account=alias,
            BackupPlanId=pid,
            VersionId=vid,
        )["BackupPlan"]
        if not plan:
            continue

        sels = _safe_aws_call(
            backup_client.list_backup_selections,
            default={"BackupSelectionsList": []},
            account=alias,
            BackupPlanId=pid,
        )["BackupSelectionsList"]
        sel_map: Dict[str, Any] = {}
        for sel in sels:
            sid = sel["SelectionId"]
            cfg = _safe_aws_call(
                backup_client.get_backup_selection,
                default={"BackupSelection": {}},
                account=alias,
                BackupPlanId=pid,
                SelectionId=sid,
            )["BackupSelection"]
            sel_map[sid] = cfg

        for rule in plan.get("Rules", []):
            expr = rule.get("ScheduleExpression", "")
            freq, det = humanise_schedule(expr)
            tz = rule.get(
                "ScheduleExpressionTimezone",
                REGION_TZ.get(backup_client.meta.region_name, "UTC"),
            )
            vault = rule.get("TargetBackupVaultName", "")
            jobs = _safe_aws_call(
                backup_client.list_backup_jobs,
                default={"BackupJobs": []},
                account=alias,
                ByBackupVaultName=vault,
                ByState="COMPLETED",
                MaxResults=1,
            )["BackupJobs"]
            last = jobs[0] if jobs else {}
            last_exec = (
                to_local(
                    last.get("CompletionDate") or last.get("CreationDate"),
                    backup_client.meta.region_name,
                )
                if last
                else ""
            )

            for cfg in sel_map.values():
                out.append(
                    {
                        "PlanName": plan.get("BackupPlanName", ""),
                        "PlanId": pid,
                        "PlanArn": summary.get("BackupPlanArn", ""),
                        "PlanCreationDate": created,
                        "RuleName": rule.get("RuleName", ""),
                        "Schedule": freq,
                        "Details": det,
                        "Timezone": tz,
                        "LastExecutionDate": last_exec,
                        "VaultName": vault,
                        "SelectionName": cfg.get("SelectionName", ""),
                        "IamRole": cfg.get("IamRoleArn", ""),
                        "Resources": cfg.get("Resources", []),
                        "ResourceTags": cfg.get("ListOfTags", []),
                        "Region": backup_client.meta.region_name,
                        "AccountAlias": alias,
                    }
                )
    return out


# --------------------------------------------------------------------------
# WAFv2 & CLASSIC COLLECTORS
# --------------------------------------------------------------------------
def get_waf_v2_details(wafv2_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    params = {"Scope": "REGIONAL"}
    # manual pagination
    while True:
        resp = _safe_aws_call(
            wafv2_client.list_web_acls, default={"WebACLs": []}, account=alias, **params
        )
        acls = resp.get("WebACLs", [])
        for acl in acls:
            full = _safe_aws_call(
                wafv2_client.get_web_acl,
                default={"WebACL": {}},
                account=alias,
                Name=acl["Name"],
                Scope="REGIONAL",
                Id=acl["Id"],
            )["WebACL"]
            rules = full.get("Rules", [])
            out.append(
                {
                    "Name": acl["Name"],
                    "WebACLId": acl["Id"],
                    "RuleCount": len(rules),
                    "Rules": [r.get("Name") for r in rules],
                    "Region": wafv2_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
        marker = resp.get("NextMarker")
        if not marker:
            break
        params["NextMarker"] = marker
    return out


def get_waf_classic_details(
    wafc_client: BaseClient, alias: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    params: Dict[str, Any] = {}
    while True:
        resp = _safe_aws_call(
            wafc_client.list_web_acls, default={"WebACLs": []}, account=alias, **params
        )
        for acl in resp.get("WebACLs", []):
            det = _safe_aws_call(
                wafc_client.get_web_acl,
                default={"WebACL": {}},
                account=alias,
                WebACLId=acl["WebACLId"],
            )["WebACL"]
            rules = det.get("Rules", [])
            out.append(
                {
                    "Name": det.get("Name", ""),
                    "WebACLId": acl["WebACLId"],
                    "RuleCount": len(rules),
                    "Rules": [r.get("RuleId", "") for r in rules],
                    "Region": wafc_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
        marker = resp.get("NextMarker")
        if not marker:
            break
        params["NextMarker"] = marker
    return out


# --------------------------------------------------------------------------
# ALB / NLB COLLECTOR
# --------------------------------------------------------------------------
def get_alb_details(elbv2_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    lbs = [
        lb
        for page in _safe_paginator(
            elbv2_client.get_paginator("describe_load_balancers").paginate,
            account=alias,
        )
        for lb in page.get("LoadBalancers", [])
    ]
    for chunk in (lbs[i : i + 20] for i in range(0, len(lbs), 20)):
        arns = [lb["LoadBalancerArn"] for lb in chunk]
        tags = _safe_aws_call(
            elbv2_client.describe_tags,
            default={"TagDescriptions": []},
            account=alias,
            ResourceArns=arns,
        )["TagDescriptions"]
        tag_map = {
            td["ResourceArn"]: {t["Key"]: t["Value"] for t in td.get("Tags", [])}
            for td in tags
        }

        for lb in chunk:
            arn = lb["LoadBalancerArn"]
            listeners = [
                {"Port": l["Port"], "Protocol": l["Protocol"]}
                for page in _safe_paginator(
                    elbv2_client.get_paginator("describe_listeners").paginate,
                    account=alias,
                    LoadBalancerArn=arn,
                )
                for l in page.get("Listeners", [])
            ]
            tgs = _safe_aws_call(
                elbv2_client.describe_target_groups,
                default={"TargetGroups": []},
                account=alias,
                LoadBalancerArn=arn,
            )["TargetGroups"]
            out.append(
                {
                    "LoadBalancerName": lb["LoadBalancerName"],
                    "DNSName": lb.get("DNSName", ""),
                    "Type": lb.get("Type", ""),
                    "Scheme": lb.get("Scheme", ""),
                    "VpcId": lb.get("VpcId", ""),
                    "State": lb.get("State", {}).get("Code", ""),
                    "AvailabilityZones": [
                        az.get("ZoneName", "") for az in lb.get("AvailabilityZones", [])
                    ],
                    "SecurityGroups": lb.get("SecurityGroups", []),
                    "Tags": tag_map.get(arn, {}),
                    "Listeners": listeners,
                    "TargetGroups": [tg["TargetGroupName"] for tg in tgs],
                    "Region": elbv2_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


# --------------------------------------------------------------------------
# LAMBDA COLLECTOR
# --------------------------------------------------------------------------
def get_lambda_details(lambda_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for page in _safe_paginator(
        lambda_client.get_paginator("list_functions").paginate, account=alias
    ):
        for fn in page.get("Functions", []):
            name, arn = fn["FunctionName"], fn["FunctionArn"]

            cfg = _safe_aws_call(
                lambda_client.get_function_configuration,
                default={},
                account=alias,
                FunctionName=name,
            )

            state = cfg.get("State", "")
            lastupd = cfg.get("LastUpdateStatus", "")
            kms = cfg.get("KMSKeyArn", "")
            hr_size = human_size(fn.get("CodeSize", 0))

            # one (non-paginated) call for tags is fine
            tag_resp = _safe_aws_call(
                lambda_client.list_tags,
                default={"Tags": {}},
                account=alias,
                Resource=arn,
            )
            tags = tag_resp.get("Tags", {})

            vpc = fn.get("VpcConfig", {})
            env = fn.get("Environment", {}).get("Variables", {})

            out.append(
                {
                    "FunctionName": name,
                    "FunctionArn": arn,
                    "Runtime": fn.get("Runtime", ""),
                    "Handler": fn.get("Handler", ""),
                    "Role": fn.get("Role", ""),
                    "Description": fn.get("Description", ""),
                    "MemorySize": fn.get("MemorySize", 0),
                    "Timeout": fn.get("Timeout", 0),
                    "PackageType": fn.get("PackageType", ""),
                    "Architectures": fn.get("Architectures", []),
                    "TracingMode": cfg.get("TracingConfig", {}).get("Mode", ""),
                    "State": state,
                    "LastUpdateStatus": lastupd,
                    "LastModified": to_local(
                        fn.get("LastModified"),
                        lambda_client.meta.region_name,
                    ),
                    "KMSKeyArn": kms,
                    "CodeSize": hr_size,
                    "VpcSecurityGroupIds": vpc.get("SecurityGroupIds", []),
                    "VpcSubnetIds": vpc.get("SubnetIds", []),
                    "EnvironmentVars": env,
                    "Tags": tags,
                    "Region": lambda_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


# --------------------------------------------------------------------------
# Lightsail COLLECTOR
# --------------------------------------------------------------------------
def get_lightsail_details(ls_client: BaseClient, alias: str) -> List[Dict[str, Any]]:
    """
    Return metadata for every Lightsail resource the caller can see.
    """
    out: List[Dict[str, Any]] = []
    region = ls_client.meta.region_name

    # --- Instances ---
    for page in _safe_paginator(
        ls_client.get_paginator("get_instances").paginate, account=alias
    ):
        for inst in page.get("instances", []):
            out.append(
                {
                    "ResourceType": "Instance",
                    "Name": inst.get("name"),
                    "Arn": inst.get("arn"),
                    "State": inst.get("state", {}).get("name"),
                    "Location": inst.get("location", {}).get("availabilityZone"),
                    "BlueprintOrEngine": inst.get("blueprintName"),
                    "BundleId": inst.get("bundleId"),
                    "IpOrDnsName": inst.get("publicIpAddress"),
                    "CreatedAt": to_local(inst.get("createdAt"), region),
                    "Region": region,
                    "AccountAlias": alias,
                }
            )

    # --- Databases ---
    for page in _safe_paginator(
        ls_client.get_paginator("get_relational_databases").paginate, account=alias
    ):
        for db in page.get("relationalDatabases", []):
            out.append(
                {
                    "ResourceType": "Database",
                    "Name": db.get("name"),
                    "Arn": db.get("arn"),
                    "State": db.get("state"),
                    "Location": db.get("location", {}).get("availabilityZone"),
                    "BlueprintOrEngine": db.get("relationalDatabaseBlueprintId"),
                    "BundleId": db.get("relationalDatabaseBundleId"),
                    "CreatedAt": to_local(db.get("createdAt"), region),
                    "Region": region,
                    "AccountAlias": alias,
                }
            )

    # --- Load Balancers ---
    for page in _safe_paginator(
        ls_client.get_paginator("get_load_balancers").paginate, account=alias
    ):
        for lb in page.get("loadBalancers", []):
            out.append(
                {
                    "ResourceType": "LoadBalancer",
                    "Name": lb.get("name"),
                    "Arn": lb.get("arn"),
                    "State": lb.get("state"),
                    "IpOrDnsName": lb.get("dnsName"),
                    "Location": lb.get("location", {}).get("availabilityZone"),
                    "CreatedAt": to_local(lb.get("createdAt"), region),
                    "Region": region,
                    "AccountAlias": alias,
                }
            )

    # --- Disks (Block Storage) ---
    for page in _safe_paginator(
        ls_client.get_paginator("get_disks").paginate, account=alias
    ):
        for disk in page.get("disks", []):
            out.append(
                {
                    "ResourceType": "Disk",
                    "Name": disk.get("name"),
                    "Arn": disk.get("arn"),
                    "State": disk.get("state"),
                    "SizeInGb": disk.get("sizeInGb"),
                    "AttachedTo": disk.get("attachedTo"),
                    "Location": disk.get("location", {}).get("availabilityZone"),
                    "CreatedAt": to_local(disk.get("createdAt"), region),
                    "Region": region,
                    "AccountAlias": alias,
                }
            )

    # --- Static IPs ---
    for page in _safe_paginator(
        ls_client.get_paginator("get_static_ips").paginate, account=alias
    ):
        for ip in page.get("staticIps", []):
            out.append(
                {
                    "ResourceType": "StaticIp",
                    "Name": ip.get("name"),
                    "Arn": ip.get("arn"),
                    "IpOrDnsName": ip.get("ipAddress"),
                    "AttachedTo": ip.get("attachedTo"),
                    "Location": ip.get("location", {}).get("availabilityZone"),
                    "CreatedAt": to_local(ip.get("createdAt"), region),
                    "Region": region,
                    "AccountAlias": alias,
                }
            )

    # --- Certificates ---
    for page in _safe_paginator(
        ls_client.get_paginator("get_certificates").paginate, account=alias
    ):
        for cert in page.get("certificates", []):
            out.append(
                {
                    "ResourceType": "Certificate",
                    "Name": cert.get("name"),
                    "Arn": cert.get("arn"),
                    "State": cert.get("status"),
                    "IpOrDnsName": cert.get(
                        "domainName"
                    ),  # Using this column for the domain
                    "ExpiresAt": to_local(cert.get("notAfter"), region),
                    "Region": region,
                    "AccountAlias": alias,
                }
            )

    # --- Domains ---
    for page in _safe_paginator(
        ls_client.get_paginator("get_domains").paginate, account=alias
    ):
        for domain in page.get("domains", []):
            out.append(
                {
                    "ResourceType": "Domain",
                    "Name": domain.get("name"),
                    "Arn": domain.get("arn"),
                    "Location": domain.get("location", {}).get("availabilityZone"),
                    "CreatedAt": to_local(domain.get("createdAt"), region),
                    "Region": region,
                    "AccountAlias": alias,
                }
            )

    return out


# --------------------------------------------------------------------------
# CloudWatch Logs & Alarms COLLECTOR
# --------------------------------------------------------------------------
def get_cloudwatch_logs_details(
    logs_client: BaseClient, alias: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for g in [
        g
        for page in _safe_paginator(
            logs_client.get_paginator("describe_log_groups").paginate, account=alias
        )
        for g in page.get("logGroups", [])
    ]:
        created = datetime.fromtimestamp(g.get("creationTime", 0) / 1000, timezone.utc)
        mfilters = _safe_aws_call(
            logs_client.describe_metric_filters,
            default={"metricFilters": []},
            account=alias,
            logGroupName=g["logGroupName"],
        )["metricFilters"]
        out.append(
            {
                "LogGroupName": g["logGroupName"],
                "RetentionInDays": g.get("retentionInDays", ""),
                "Size": human_size(g.get("storedBytes")),
                "CreationTime": to_local(created, logs_client.meta.region_name),
                "MetricFilterCount": len(mfilters),
                "Region": logs_client.meta.region_name,
                "AccountAlias": alias,
            }
        )
    return out


def get_cloudwatch_alarms_details(
    cw_client: BaseClient, alias: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in _safe_paginator(
        cw_client.get_paginator("describe_alarms").paginate, account=alias
    ):
        for a in page.get("MetricAlarms", []):
            out.append(
                {
                    "AlarmName": a["AlarmName"],
                    "AlarmType": "Metric",
                    "MetricName": a.get("MetricName", ""),
                    "Namespace": a.get("Namespace", ""),
                    "Statistic": a.get("Statistic", ""),
                    "Period": a.get("Period", ""),
                    "Threshold": a.get("Threshold", ""),
                    "ComparisonOperator": a.get("ComparisonOperator", ""),
                    "EvaluationPeriods": a.get("EvaluationPeriods", ""),
                    "DatapointsToAlarm": a.get("DatapointsToAlarm", ""),
                    "ActionsEnabled": a.get("ActionsEnabled", False),
                    "AlarmActions": a.get("AlarmActions", []),
                    "InsufficientDataActions": a.get("InsufficientDataActions", []),
                    "OKActions": a.get("OKActions", []),
                    "Region": cw_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
        for c in page.get("CompositeAlarms", []):
            out.append(
                {
                    "AlarmName": c["AlarmName"],
                    "AlarmType": "Composite",
                    "MetricName": "",
                    "Namespace": "",
                    "Statistic": "",
                    "Period": "",
                    "Threshold": "",
                    "ComparisonOperator": "",
                    "EvaluationPeriods": "",
                    "DatapointsToAlarm": "",
                    "ActionsEnabled": c.get("ActionsEnabled", False),
                    "AlarmActions": c.get("AlarmActions", []),
                    "InsufficientDataActions": c.get("InsufficientDataActions", []),
                    "OKActions": c.get("OKActions", []),
                    "Region": cw_client.meta.region_name,
                    "AccountAlias": alias,
                }
            )
    return out


# --------------------------------------------------------------------------
# IAM Collector Helpers
# --------------------------------------------------------------------------
def _get_inline_policies(
    iam: BaseClient, entity_type: str, entity_name: str
) -> tuple[list[str], dict]:
    """Helper to get inline policies for any IAM entity."""
    method_map = {
        "user": (iam.list_user_policies, iam.get_user_policy),
        "role": (iam.list_role_policies, iam.get_role_policy),
        "group": (iam.list_group_policies, iam.get_group_policy),
    }
    list_method, get_method = method_map[entity_type]

    # Get list of policy names
    list_kwargs = {f"{entity_type.capitalize()}Name": entity_name}
    inlnames = list_method(**list_kwargs).get("PolicyNames", [])

    # Get policy documents
    inl_policy_docs = {}
    for polname in inlnames:
        get_kwargs = {
            f"{entity_type.capitalize()}Name": entity_name,
            "PolicyName": polname,
        }
        pol_doc = get_method(**get_kwargs).get("PolicyDocument")
        inl_policy_docs[polname] = pol_doc

    return inlnames, inl_policy_docs


def _get_attached_policies(
    iam: BaseClient, entity_type: str, entity_name: str
) -> list[str]:
    """Helper to get attached policies for any IAM entity."""
    method_map = {
        "user": iam.list_attached_user_policies,
        "role": iam.list_attached_role_policies,
        "group": iam.list_attached_group_policies,
    }
    method = method_map[entity_type]

    kwargs = {f"{entity_type.capitalize()}Name": entity_name}
    ats = method(**kwargs).get("AttachedPolicies", [])
    return [p["PolicyArn"] for p in ats]


# --------------------------------------------------------------------------
# IAM Collectors
# --------------------------------------------------------------------------
def get_iam_users_details(iam: BaseClient, account_alias: str) -> list[dict[str, Any]]:
    results = []
    paginator = iam.get_paginator("list_users")
    for page in paginator.paginate():
        for user in page.get("Users", []):
            user_name = user["UserName"]
            user_summary = {
                "UserName": user_name,
                "UserId": user["UserId"],
                "Arn": user["Arn"],
                "CreateDate": to_local(user.get("CreateDate"), iam.meta.region_name),
                "PasswordLastUsed": to_local(
                    user.get("PasswordLastUsed"), iam.meta.region_name
                ),
                "Groups": [],
                "AttachedPolicies": [],
                "InlinePolicies": [],
                # Remove InlinePolicyDocs to reduce redundancy
                "AccountAlias": account_alias,
            }

            # Groups
            groups = iam.list_groups_for_user(UserName=user_name).get("Groups", [])
            user_summary["Groups"] = [g["GroupName"] for g in groups]

            # Attached managed policies
            ats = iam.list_attached_user_policies(UserName=user_name).get(
                "AttachedPolicies", []
            )
            user_summary["AttachedPolicies"] = [p["PolicyArn"] for p in ats]

            # Inline policies (names only, no docs)
            user_summary["InlinePolicies"] = iam.list_user_policies(
                UserName=user_name
            ).get("PolicyNames", [])

            results.append(user_summary)
    return results


def get_iam_roles_details(iam: BaseClient, account_alias: str) -> list[dict[str, Any]]:
    results = []
    paginator = iam.get_paginator("list_roles")
    for page in paginator.paginate():
        for role in page.get("Roles", []):
            role_name = role["RoleName"]

            # Extract service principals from trust policy
            trust_policy = role.get("AssumeRolePolicyDocument", {})
            service_principals = []
            if trust_policy:
                for stmt in trust_policy.get("Statement", []):
                    principal = stmt.get("Principal", {})
                    if "Service" in principal:
                        if isinstance(principal["Service"], list):
                            service_principals.extend(principal["Service"])
                        else:
                            service_principals.append(principal["Service"])

            role_summary = {
                "RoleName": role_name,
                "RoleId": role["RoleId"],
                "Arn": role["Arn"],
                "CreateDate": to_local(role.get("CreateDate"), iam.meta.region_name),
                "ServicePrincipals": service_principals,  # Replace full trust policy with just services
                "AttachedPolicies": [],
                "InlinePolicies": [],
                # Remove InlinePolicyDocs to reduce redundancy
                "AccountAlias": account_alias,
            }

            # Attached managed policies
            ats = iam.list_attached_role_policies(RoleName=role_name).get(
                "AttachedPolicies", []
            )
            role_summary["AttachedPolicies"] = [p["PolicyArn"] for p in ats]

            # Inline policies (names only)
            role_summary["InlinePolicies"] = iam.list_role_policies(
                RoleName=role_name
            ).get("PolicyNames", [])

            results.append(role_summary)
    return results


def get_iam_groups_details(iam: BaseClient, account_alias: str) -> list[dict[str, Any]]:
    results = []
    paginator = iam.get_paginator("list_groups")
    for page in paginator.paginate():
        for group in page.get("Groups", []):
            group_name = group["GroupName"]
            group_summary = {
                "GroupName": group_name,
                "GroupId": group["GroupId"],
                "Arn": group["Arn"],
                "CreateDate": to_local(group.get("CreateDate"), iam.meta.region_name),
                "AttachedPolicies": [],
                "InlinePolicies": [],
                # Remove InlinePolicyDocs to reduce redundancy
                "Members": [],
                "AccountAlias": account_alias,
            }

            # Attached managed policies
            ats = iam.list_attached_group_policies(GroupName=group_name).get(
                "AttachedPolicies", []
            )
            group_summary["AttachedPolicies"] = [p["PolicyArn"] for p in ats]

            # Inline policies
            group_summary["InlinePolicies"] = iam.list_group_policies(
                GroupName=group_name
            ).get("PolicyNames", [])

            # Members
            mem = iam.get_group(GroupName=group_name).get("Users", [])
            group_summary["Members"] = [u["UserName"] for u in mem]

            results.append(group_summary)
    return results


def get_iam_policies_details(
    iam: BaseClient, account_alias: str
) -> list[dict[str, Any]]:
    results = []
    paginator = iam.get_paginator("list_policies")

    # Comprehensive AWS service prefix mapping
    service_prefixes = {
        # AI/ML Services
        "Bedrock": "Bedrock",
        "Comprehend": "Comprehend",
        "Forecast": "Forecast",
        "Lex": "Lex",
        "Polly": "Polly",
        "Rekognition": "Rekognition",
        "SageMaker": "SageMaker",
        "Textract": "Textract",
        "Transcribe": "Transcribe",
        "Translate": "Translate",
        "MachineLearning": "Machine Learning",
        "Personalize": "Personalize",
        "Kendra": "Kendra",
        "TensorFlow": "TensorFlow",
        "DeepLens": "DeepLens",
        "DeepRacer": "DeepRacer",
        "FraudDetector": "Fraud Detector",
        "CodeWhisperer": "CodeWhisperer",
        "Q": "Q",
        # Analytics
        "Athena": "Athena",
        "CloudSearch": "CloudSearch",
        "DataBrew": "DataBrew",
        "EMR": "EMR",
        "ElasticMapReduce": "EMR",
        "OpenSearchService": "OpenSearch Service",
        "Elasticsearch": "Elasticsearch",
        "Kinesis": "Kinesis",
        "QuickSight": "QuickSight",
        "Redshift": "Redshift",
        "Glue": "Glue",
        "DataPipeline": "Data Pipeline",
        "LakeFormation": "Lake Formation",
        "MSK": "MSK",
        "ManagedStreamingKafka": "MSK",
        # Compute
        "EC2": "EC2",
        "AutoScaling": "Auto Scaling",
        "Lambda": "Lambda",
        "ElasticBeanstalk": "Elastic Beanstalk",
        "ECS": "ECS",
        "EKS": "EKS",
        "Batch": "Batch",
        "Fargate": "Fargate",
        "LightSail": "LightSail",
        "AppRunner": "App Runner",
        "Serverless": "Serverless",
        # Containers
        "ECR": "ECR",
        "ContainerRegistry": "ECR",
        "EKS": "EKS",
        "ECS": "ECS",
        "AppMesh": "App Mesh",
        # Database
        "RDS": "RDS",
        "DynamoDB": "DynamoDB",
        "DocumentDB": "DocumentDB",
        "ElastiCache": "ElastiCache",
        "Neptune": "Neptune",
        "QLDB": "QLDB",
        "Timestream": "Timestream",
        "Keyspaces": "Keyspaces",
        "MemoryDB": "MemoryDB",
        "Aurora": "Aurora",
        # Developer Tools
        "CodeBuild": "CodeBuild",
        "CodeCommit": "CodeCommit",
        "CodeDeploy": "CodeDeploy",
        "CodePipeline": "CodePipeline",
        "CodeStar": "CodeStar",
        "CodeArtifact": "CodeArtifact",
        "CodeGuru": "CodeGuru",
        "Cloud9": "Cloud9",
        "XRay": "X-Ray",
        # Management & Governance
        "CloudWatch": "CloudWatch",
        "CloudFormation": "CloudFormation",
        "CloudTrail": "CloudTrail",
        "Config": "Config",
        "Organizations": "Organizations",
        "SSM": "Systems Manager",
        "SystemsManager": "Systems Manager",
        "ControlTower": "Control Tower",
        "License": "License Manager",
        "ServiceCatalog": "Service Catalog",
        "Chatbot": "Chatbot",
        "Console": "Console",
        "Health": "Health Dashboard",
        "AutoScaling": "Auto Scaling",
        "OpsWorks": "OpsWorks",
        # Networking & Content Delivery
        "VPC": "VPC",
        "CloudFront": "CloudFront",
        "Route53": "Route 53",
        "APIGateway": "API Gateway",
        "AppMesh": "App Mesh",
        "DirectConnect": "Direct Connect",
        "GlobalAccelerator": "Global Accelerator",
        "ELB": "Elastic Load Balancing",
        "ElasticLoadBalancing": "Elastic Load Balancing",
        "PrivateNetworks": "Private Networks",
        "NetworkManager": "Network Manager",
        "PrivateLink": "PrivateLink",
        "Transit": "Transit Gateway",
        # Security, Identity & Compliance
        "IAM": "IAM",
        "Cognito": "Cognito",
        "GuardDuty": "GuardDuty",
        "Inspector": "Inspector",
        "Macie": "Macie",
        "SecretsManager": "Secrets Manager",
        "SecurityHub": "Security Hub",
        "Shield": "Shield",
        "SingleSignOn": "Single Sign-On",
        "SSO": "Single Sign-On",
        "WAF": "WAF",
        "Firewall": "Firewall Manager",
        "Directory": "Directory Service",
        "KMS": "KMS",
        "IdentityCenter": "IAM Identity Center",
        "STS": "Security Token Service",
        "ARCZonalShift": "ARC Zonal Shift",
        "Artifact": "Artifact",
        "CertificateManager": "Certificate Manager",
        "ACM": "Certificate Manager",
        "Detective": "Detective",
        "RAM": "Resource Access Manager",
        "Verified": "Verified Permissions",
        "IVS": "IVS",
        # Storage
        "S3": "S3",
        "EFS": "EFS",
        "EBS": "EBS",
        "FSx": "FSx",
        "StorageGateway": "Storage Gateway",
        "Backup": "Backup",
        "SnowFamily": "Snow Family",
        "SimSpace": "SimSpace Weaver",
        "Transfer": "Transfer Family",
        # Application Integration
        "SNS": "SNS",
        "SQS": "SQS",
        "SWF": "SWF",
        "StepFunctions": "Step Functions",
        "AppFlow": "AppFlow",
        "AppSync": "AppSync",
        "EventBridge": "EventBridge",
        "MQ": "MQ",
        "SES": "SES",
        "SimpleEmail": "SES",
        # Business Applications
        "Connect": "Connect",
        "Honeycode": "Honeycode",
        "Pinpoint": "Pinpoint",
        "SimpleWorkflow": "SWF",
        "WorkDocs": "WorkDocs",
        "WorkMail": "WorkMail",
        "Chime": "Chime",
        "Wickr": "Wickr",
        # End User Computing
        "WorkSpaces": "WorkSpaces",
        "AppStream": "AppStream",
        "WorkLink": "WorkLink",
        # IoT
        "IoT": "IoT",
        "Greengrass": "Greengrass",
        "FreeRTOS": "FreeRTOS",
        # Blockchain
        "ManagedBlockchain": "Managed Blockchain",
        "QuantumLedger": "QLDB",
        # Satellite
        "GroundStation": "Ground Station",
        # Robotics
        "RoboMaker": "RoboMaker",
        # Game Development
        "GameLift": "GameLift",
        # AR & VR
        "Sumerian": "Sumerian",
        # Customer Engagement
        "Connect": "Connect",
        "PinpointSMS": "Pinpoint SMS",
        # Media Services
        "MediaConvert": "MediaConvert",
        "MediaLive": "MediaLive",
        "MediaPackage": "MediaPackage",
        "MediaStore": "MediaStore",
        "MediaTailor": "MediaTailor",
        "Elemental": "Elemental",
        # Migration & Transfer
        "ApplicationDiscovery": "Application Discovery",
        "DMS": "Database Migration Service",
        "DataSync": "DataSync",
        "MigrationHub": "Migration Hub",
        "SMS": "Server Migration Service",
        "Transfer": "Transfer Family",
        # Quantum Computing
        "Braket": "Braket",
        # Billing & Cost Management
        "Budgets": "Budgets",
        "CostExplorer": "Cost Explorer",
        "Pricing": "Pricing Calculator",
    }

    # Process both AWS and Customer managed policies
    for scope in ["Local", "AWS"]:
        policy_type = "Customer Managed" if scope == "Local" else "AWS Managed"
        for page in paginator.paginate(Scope=scope):
            for policy in page.get("Policies", []):
                # Skip unattached AWS managed policies
                if scope == "AWS" and policy["AttachmentCount"] == 0:
                    continue

                # Extract service category from policy name
                service_category = ""
                if scope == "AWS":
                    name = policy["PolicyName"]

                    # Remove common prefixes
                    if name.startswith("Amazon"):
                        name = name[6:]
                    elif name.startswith("AWS"):
                        name = name[3:]

                    # Match against known service prefixes
                    for prefix, service_name in service_prefixes.items():
                        if name.startswith(prefix):
                            service_category = service_name
                            break

                    # If not matched by known prefixes, try to extract from ARN
                    if not service_category:
                        # Example ARN: arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
                        arn_parts = policy["Arn"].split("/")
                        if len(arn_parts) > 1 and "service-role" in arn_parts:
                            role_part = arn_parts[-1]
                            for prefix, service_name in service_prefixes.items():
                                if prefix in role_part:
                                    service_category = service_name
                                    break

                policy_summary = {
                    "PolicyName": policy["PolicyName"],
                    "PolicyId": policy["PolicyId"],
                    "Arn": policy["Arn"],
                    "AttachmentCount": policy["AttachmentCount"],
                    "DefaultVersionId": policy["DefaultVersionId"],
                    "PolicyType": policy_type,
                    "ServiceCategory": service_category,
                    "AttachmentEntities": [],
                    "CreateDate": to_local(
                        policy.get("CreateDate"), iam.meta.region_name
                    ),
                    "UpdateDate": to_local(
                        policy.get("UpdateDate"), iam.meta.region_name
                    ),
                    "PolicyDocument": {},
                    "AccountAlias": account_alias,
                }

                # Get policy document
                try:
                    ver = iam.get_policy_version(
                        PolicyArn=policy["Arn"],
                        VersionId=policy["DefaultVersionId"],
                    )
                    # For AWS managed policies, extract action patterns without full document
                    if scope == "AWS":
                        actions = []
                        for stmt in ver["PolicyVersion"]["Document"].get(
                            "Statement", []
                        ):
                            if "Action" in stmt:
                                if isinstance(stmt["Action"], list):
                                    actions.extend(stmt["Action"])
                                else:
                                    actions.append(stmt["Action"])
                        policy_summary["ActionPatterns"] = list(set(actions))
                    else:
                        # Store full document only for customer managed policies
                        policy_summary["PolicyDocument"] = ver["PolicyVersion"][
                            "Document"
                        ]
                except Exception:
                    pass

                # Find where attached
                attached = iam.list_entities_for_policy(PolicyArn=policy["Arn"])
                for typ in ("PolicyGroups", "PolicyUsers", "PolicyRoles"):
                    for ent in attached.get(typ, []):
                        entity_type = typ.replace("Policy", "")[:-1]  # e.g. "Group"
                        entity_name_key = {
                            "Group": "GroupName",
                            "User": "UserName",
                            "Role": "RoleName",
                        }[entity_type]

                        policy_summary["AttachmentEntities"].append(
                            {"Type": entity_type, "Name": ent[entity_name_key]}
                        )

                results.append(policy_summary)

    return results


# --------------------------------------------------------------------------
# GLOBAL SERVICE REGISTRY
# --------------------------------------------------------------------------
GLOBAL_SERVICES = {
    "Route53": ("route53", get_route53_details),
    "SES": ("ses", get_ses_details),
    "SNS": ("sns", get_sns_details),
}

# --------------------------------------------------------------------------
# EXCEL COLUMNS & UNIQUE KEYS
# --------------------------------------------------------------------------
SERVICE_COLUMNS: Dict[str, List[str]] = {
    "Summary": ["AccountId", "AccountAlias", "TotalResources"],
    "ACM": [
        "DomainName",
        "CertificateArn",
        "Status",
        "Type",
        "InUse",
        "Issued",
        "Expires",
        "RenewalEligibility",
        "SubjectAlternativeNames",
        "Region",
        "AccountAlias",
    ],
    "ALB": [
        "LoadBalancerName",
        "DNSName",
        "Type",
        "Scheme",
        "VpcId",
        "State",
        "AvailabilityZones",
        "SecurityGroups",
        "Tags",
        "Listeners",
        "TargetGroups",
        "Region",
        "AccountAlias",
    ],
    "Backup": [
        "PlanName",
        "PlanId",
        "PlanArn",
        "PlanCreationDate",
        "LastExecutionDate",
        "RuleName",
        "Schedule",
        "Details",
        "Timezone",
        "VaultName",
        "SelectionName",
        "IamRole",
        "Resources",
        "ResourceTags",
        "Region",
        "AccountAlias",
    ],
    "CloudWatchAlarms": [
        "AlarmName",
        "AlarmType",
        "MetricName",
        "Namespace",
        "Statistic",
        "Period",
        "Threshold",
        "ComparisonOperator",
        "EvaluationPeriods",
        "DatapointsToAlarm",
        "ActionsEnabled",
        "AlarmActions",
        "InsufficientDataActions",
        "OKActions",
        "Region",
        "AccountAlias",
    ],
    "CloudWatchLogs": [
        "LogGroupName",
        "RetentionInDays",
        "Size",
        "CreationTime",
        "MetricFilterCount",
        "Region",
        "AccountAlias",
    ],
    "EC2": [
        "Name",
        "InstanceId",
        "InstanceType",
        "KeyPair",
        "vCPUs",
        "Memory",
        "NetworkPerformance",
        "AvailabilityZone",
        "PublicIP",
        "PrivateIP",
        "IPType",
        "EBSVolumes",
        "OS",
        "AWSBackup",
        "Region",
        "AccountAlias",
    ],
    "EC2ReservedInstances": [
        "ReservedInstancesId",
        "InstanceType",
        "Scope",
        "InstanceCount",
        "OfferingType",
        "ProductDescription",
        "Duration",
        "FixedPrice",
        "UsagePrice",
        "CurrencyCode",
        "StartTime",
        "State",
        "Region",
        "AccountAlias",
    ],
    "EventBridge": [
        "ScheduleName",
        "GroupName",
        "State",
        "Expression",
        "Timezone",
        "Frequency",
        "Details",
        "TargetArn",
        "Input",
        "Region",
        "AccountAlias",
    ],
    "EventBridgeScheduler": [
        "ScheduleName",
        "GroupName",
        "State",
        "Expression",
        "Timezone",
        "Frequency",
        "Details",
        "TargetArn",
        "Input",
        "Region",
        "AccountAlias",
    ],
    "IAMGroups": [
        "GroupName",
        "GroupId",
        "Arn",
        "CreateDate",
        "AttachedPolicies",
        "InlinePolicies",
        "Members",
        "AccountAlias",
    ],
    "IAMPolicies": [
        "PolicyName",
        "PolicyId",
        "Arn",
        "AttachmentCount",
        "DefaultVersionId",
        "PolicyType",
        "ServiceCategory",
        "ActionPatterns",
        "AttachmentEntities",
        "CreateDate",
        "UpdateDate",
        "PolicyDocument",
        "AccountAlias",
    ],
    "IAMRoles": [
        "RoleName",
        "RoleId",
        "Arn",
        "CreateDate",
        "ServicePrincipals",
        "AttachedPolicies",
        "InlinePolicies",
        "AccountAlias",
    ],
    "IAMUsers": [
        "UserName",
        "UserId",
        "Arn",
        "CreateDate",
        "PasswordLastUsed",
        "Groups",
        "AttachedPolicies",
        "InlinePolicies",
        "AccountAlias",
    ],
    "KMS": [
        "KeyId",
        "Description",
        "Enabled",
        "KeyState",
        "KeyManager",
        "KeySpec",
        "KeyUsage",
        "Origin",
        "CreationDate",
        "DeletionDate",
        "ValidTo",
        "MultiRegion",
        "PendingDeletion",
        "PendingWindowInDays",
        "AliasNames",
        "Tags",
        "RotationEnabled",
        "GrantsCount",
        "Region",
        "AccountAlias",
    ],
    "Lambda": [
        "FunctionName",
        "FunctionArn",
        "Runtime",
        "Handler",
        "Role",
        "Description",
        "MemorySize",
        "Timeout",
        "PackageType",
        "Architectures",
        "TracingMode",
        "State",
        "LastUpdateStatus",
        "LastModified",
        "KMSKeyArn",
        "CodeSize",
        "VpcSecurityGroupIds",
        "VpcSubnetIds",
        "EnvironmentVars",
        "Tags",
        "Region",
        "AccountAlias",
    ],
    "Lightsail": [
        "ResourceType",
        "Name",
        "Arn",
        "State",
        "Location",
        "BlueprintOrEngine",
        "BundleId",
        "IpOrDnsName",
        "AttachedTo",
        "SizeInGb",
        "ExpiresAt",
        "CreatedAt",
        "Region",
        "AccountAlias",
    ],
    "RDS": [
        "DBInstanceIdentifier",
        "Engine",
        "DBInstanceClass",
        "vCPUs",
        "Memory",
        "MultiAZ",
        "PubliclyAccessible",
        "StorageType",
        "AllocatedStorage",
        "EndpointAddress",
        "EndpointPort",
        "InstanceCreateTime",
        "LicenseModel",
        "VpcSecurityGroupIds",
        "DBSubnetGroup",
        "Tags",
        "Region",
        "AccountAlias",
    ],
    "RDSReservedInstances": [
        "ReservedDBInstanceId",
        "DBInstanceClass",
        "Duration",
        "FixedPrice",
        "UsagePrice",
        "CurrencyCode",
        "StartTime",
        "State",
        "MultiAZ",
        "OfferingType",
        "ProductDescription",
        "Region",
        "AccountAlias",
    ],
    "Route53": [
        "Name",
        "Id",
        "Config",
        "ResourceRecordSetCount",
        "RecordTypes",
        "Tags",
        "VPCAssociations",
        "HealthChecks",
        "DNSSECStatus",
        "DelegationSet",
        "Region",
        "AccountAlias",
    ],
    "S3": [
        "BucketName",
        "CreationDate",
        "Region",
        "Size",
        "ObjectCount",
        "LastMetricsUpdate",
        "MetricsCalculationMethod",
        "Versioning",
        "Encryption",
        "PublicAccess",
        "PolicyStatus",
        "LifecycleRules",
        "Tags",
        "AccountAlias",
    ],
    "SavingsPlans": [
        "SavingsPlanId",
        "SavingsPlanArn",
        "State",
        "Start",
        "End",
        "Term",
        "PaymentOption",
        "PlanType",
        "Region",
        "AccountAlias",
    ],
    "SES": [
        "Identity",
        "VerificationStatus",
        "Region",
        "AccountAlias",
    ],
    "SNS": [
        "TopicArn",
        "TopicName",
        "SubscriptionCount",
        "Subscriptions",
        "Region",
        "AccountAlias",
    ],
    "WAFClassic": [
        "Name",
        "WebACLId",
        "RuleCount",
        "Rules",
        "Region",
        "AccountAlias",
    ],
    "WAFv2": [
        "Name",
        "WebACLId",
        "RuleCount",
        "Rules",
        "Region",
        "AccountAlias",
    ],
    "VPN": [
        "VpnConnectionId",
        "Name",
        "State",
        "CustomerGateway",
        "CustomerGatewaySource",
        "TunnelOutsideIps",
        "Region",
        "AccountAlias",
    ],
}

UNIQUE_KEYS: Dict[str, Union[str, Tuple[str, ...]]] = {
    "ACM": "CertificateArn",
    "ALB": "LoadBalancerName",
    "Backup": ("PlanId", "RuleName", "SelectionName"),
    "CloudWatchAlarms": "AlarmName",
    "CloudWatchLogs": "LogGroupName",
    "EC2": "InstanceId",
    "EC2ReservedInstances": "ReservedInstancesId",
    "EventBridge": "ScheduleName",
    "EventBridgeScheduler": "ScheduleName",
    "IAMGroups": "GroupId",
    "IAMPolicies": "PolicyId",
    "IAMRoles": "RoleId",
    "IAMUsers": "UserId",
    "KMS": "KeyId",
    "Lambda": "FunctionArn",
    "Lightsail": "Arn",
    "RDS": "DBInstanceIdentifier",
    "RDSReservedInstances": "ReservedDBInstanceId",
    "Route53": "Id",
    "S3": "BucketName",
    "SES": "Identity",
    "SNS": "TopicArn",
    "SavingsPlans": "SavingsPlanId",
    "VPN": "VpnConnectionId",
    "WAFClassic": "WebACLId",
    "WAFv2": "WebACLId",
}

_REGION_SCOPED_SHEETS: set[str] = {
    "KMS",
    "ACM",
    "CloudWatchLogs",
    "CloudWatchAlarms",
    "Lambda",
}


# --------------------------------------------------------------------------
# EXCEL WRITER
# --------------------------------------------------------------------------
class StreamingExcelWriter:
    def __init__(self, filename: str, export_tz: str):
        self._acct_ids: dict[str, str] = {}
        self.filename = filename
        self.export_tz = export_tz
        self.wb = Workbook()
        # remove default sheet
        self.wb.remove(self.wb.active)
        self.sheets: Dict[str, Worksheet] = {}
        # seen[(account)][sheet] = set of (unique_key, region)
        self._seen: Dict[str, Dict[str, set[Tuple[str, str]]]] = {}
        self._table_names: set[str] = set()

    @staticmethod
    def _excel_str(cell: Any, *, limit: int = 120) -> str:
        """
        Serialise a cell value for Excel width calculation.

        * `None`   → empty string
        * >`limit` → truncated with ellipsis
        * else     → str(value)
        """
        if cell is None:
            return ""
        s = str(cell)
        return (s[: limit - 3] + "...") if len(s) > limit else s

    @staticmethod
    def _logical_len(items: Union[Set[Any], Tuple[Any, ...]]) -> int:
        """
        Return the count of *logical* resources in *items*.
        Elements may be raw strings or tuples such as (resource_id, region).
        """
        if not items:
            return 0
        items = set(items)
        return len({v[0] if isinstance(v, tuple) else v for v in items})

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    def record_account(self, account_id: str, alias: str) -> None:
        """Remember the AccountId → Alias mapping once per scan."""
        self._acct_ids[account_id] = alias

    def _safe_table_name(self, sheet_name: str) -> str:
        """
        Generate a unique, Excel-legal table name (≤ 31 chars).
        """
        stem = "".join(c if c.isalnum() else "_" for c in sheet_name)
        if stem and stem[0].isdigit():
            stem = "_" + stem
        stem = stem[:_MAX_EXCEL_NAME_LEN]

        existing = self._table_names.union(self.wb.sheetnames)
        name, idx = stem, 1
        while name in existing:
            suffix = f"_{idx}"
            name = f"{stem[: _MAX_EXCEL_NAME_LEN - len(suffix)]}{suffix}"
            idx += 1

        self._table_names.add(name)
        return name

    def _get_sheet(self, name: str) -> Worksheet:
        # create on first use, write header row
        if name not in self.sheets:
            ws = self.wb.create_sheet(name[:31])
            ws.append(SERVICE_COLUMNS[name])
            self.sheets[name] = ws
        return self.sheets[name]

    def _append_serialized(
        self,
        ws: Worksheet,
        sheet: str,
        row: Dict[str, Any],
    ) -> None:
        """
        JSON-encode complex cells and append the row.
        """
        serialised = [
            (
                json.dumps(row[col], separators=(",", ":"))
                if isinstance(row.get(col), (dict, list, tuple, set))
                else row.get(col, "")
            )
            for col in SERVICE_COLUMNS[sheet]
        ]
        ws.append(serialised)

    def write_row(self, sheet: str, row: Dict[str, Any]) -> None:
        """
        Append a single row, JSON-encoding complex objects and
        de-duping within an account.
        """
        acct_id = row.get("AccountId")
        uniq_key = UNIQUE_KEYS.get(sheet)
        region = row.get("Region")

        if not (uniq_key and acct_id):
            self._append_serialized(self._get_sheet(sheet), sheet, row)
            return

        base_id = (
            tuple(row.get(k) for k in uniq_key)
            if isinstance(uniq_key, (list, tuple))
            else row.get(uniq_key)
        )

        # ensure the identifier itself is hashable
        if sheet in _REGION_SCOPED_SHEETS:
            identifier = (base_id, region)
        else:
            identifier = base_id

        seen = self._seen.setdefault(acct_id, {}).setdefault(sheet, set())
        if identifier in seen:
            return

        seen.add(identifier)
        self._append_serialized(self._get_sheet(sheet), sheet, row)

    def close(self) -> None:
        ordered = sorted(self.sheets)
        self.wb._sheets = [self.sheets[n] for n in ordered]

        # format each data sheet
        for name in ordered:
            ws = self.sheets[name]
            if ws.max_row == 1:
                continue
            ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
            tbl = Table(displayName=self._safe_table_name(name), ref=ref)
            tbl.tableStyleInfo = TableStyleInfo(
                name="TableStyleLight9", showRowStripes=True
            )
            ws.add_table(tbl)
            ws.freeze_panes = "A2"

            for idx in range(1, ws.max_column + 1):
                letter = get_column_letter(idx)
                sample = (c.value for c in ws[letter][:250])
                ws.column_dimensions[letter].width = (
                    max(len(self._excel_str(c)) for c in sample) + 2
                )

        summary = self.wb.create_sheet("Summary", 0)
        summary.append(
            [
                "Exported At:",
                datetime.now(ZoneInfo(self.export_tz)).strftime("%Y-%m-%d %H:%M:%S %Z"),
            ]
        )
        summary.append([])
        header = ["AccountId", "AccountAlias", "TotalResources"] + ordered
        summary.append(header)

        for acct_id, svc_map in self._seen.items():
            alias = self._acct_ids.get(acct_id, acct_id)
            total = sum(self._logical_len(svc_map.get(s, ())) for s in ordered)
            summary.append(
                [acct_id, alias, total]
                + [self._logical_len(svc_map.get(s, ())) for s in ordered]
            )

        ref = f"A3:{get_column_letter(summary.max_column)}{summary.max_row}"
        tbl = Table(displayName="SummaryTable", ref=ref)
        tbl.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium9", showRowStripes=True
        )
        summary.add_table(tbl)
        summary.freeze_panes = "A4"

        for idx in range(1, summary.max_column + 1):
            letter = get_column_letter(idx)
            summary.column_dimensions[letter].width = (
                max(len(str(c.value or "")) for c in summary[letter]) + 2
            )

        try:
            self.wb.save(self.filename)
        except PermissionError as exc:
            logger.error(
                "Failed to save %s (%s). Is the file open?",
                self.filename,
                exc,
                extra={"account": "-"},
            )
            raise


# --------------------------------------------------------------------------
# ACCOUNT SCAN
# --------------------------------------------------------------------------
def scan_account(
    acct_id: str,
    sess: boto3.Session,
    regions: list[str],
    include_collectors: Optional[set[str]] = None,
    exclude_collectors: Optional[set[str]] = None,
) -> tuple[dict[str, Any], str]:
    try:
        aliases = (
            sess.client("iam", region_name="us-east-1")
            .list_account_aliases()
            .get("AccountAliases", [])
        )
        alias = aliases[0] if aliases else acct_id
    except Exception:
        alias = acct_id

    s3_global = aws_client("s3", "us-east-1", sess)
    all_buckets = s3_global.list_buckets().get("Buckets", [])

    buckets_by_region: dict[str, list[dict[str, Any]]] = {r: [] for r in regions}
    for b in all_buckets:
        b_region = bucket_region(b["Name"], regions[0], sess)
        if b_region in buckets_by_region:
            buckets_by_region[b_region].append(b)

    global_block: dict[str, Any] = {}
    for sheet, (api, fn) in GLOBAL_SERVICES.items():
        if include_collectors and sheet not in include_collectors:
            continue
        if exclude_collectors and sheet in exclude_collectors:
            continue
        try:
            client = aws_client(api, "us-east-1", sess)
            global_block[sheet] = fn(client, alias)
        except Exception as exc:
            logger.error(
                "Global collector %s failed: %s", sheet, exc, extra={"account": acct_id}
            )
            global_block[sheet] = []

    if (not include_collectors or "SavingsPlans" in include_collectors) and (
        not exclude_collectors or "SavingsPlans" not in exclude_collectors
    ):
        try:
            sp_client = aws_client("savingsplans", "us-east-1", sess)
            global_block["SavingsPlans"] = get_savings_plan_details(
                sp_client, alias, sess
            )
        except Exception as exc:
            logger.error(
                "Global collector SavingsPlans failed: %s",
                exc,
                extra={"account": acct_id},
            )
            global_block["SavingsPlans"] = []

    def region_worker(region: str) -> tuple[str, dict[str, Any]]:
        def ec2_collector():
            return get_ec2_details(
                aws_client("ec2", region, sess),
                aws_client("backup", region, sess),
                alias,
                sess,
            )

        def s3_collector():
            return get_s3_details(
                aws_client("s3", region, sess),
                buckets_by_region.get(region, []),
                alias,
                region,
                sess,
            )

        collectors: dict[str, Callable[[], Any]] = {
            "ACM": lambda: get_acm_details(aws_client("acm", region, sess), alias),
            "ALB": lambda: get_alb_details(aws_client("elbv2", region, sess), alias),
            "Backup": lambda: get_backup_details(
                aws_client("backup", region, sess), alias
            ),
            "CloudWatchAlarms": lambda: get_cloudwatch_alarms_details(
                aws_client("cloudwatch", region, sess), alias
            ),
            "CloudWatchLogs": lambda: get_cloudwatch_logs_details(
                aws_client("logs", region, sess), alias
            ),
            "EC2": ec2_collector,
            "EC2ReservedInstances": lambda: get_ec2_reserved_instances(
                aws_client("ec2", region, sess), alias, sess
            ),
            "EventBridge": lambda: get_eventbridge_details(
                aws_client("events", region, sess), alias
            ),
            "EventBridgeScheduler": lambda: get_eventbridge_scheduler_details(
                aws_client("scheduler", region, sess), alias
            ),
            "IAMGroups": lambda: get_iam_groups_details(
                aws_client("iam", region, sess), alias
            ),
            "IAMRoles": lambda: get_iam_roles_details(
                aws_client("iam", region, sess), alias
            ),
            "IAMPolicies": lambda: get_iam_policies_details(
                aws_client("iam", region, sess), alias
            ),
            "IAMUsers": lambda: get_iam_users_details(
                aws_client("iam", region, sess), alias
            ),
            "KMS": lambda: get_kms_details(aws_client("kms", region, sess), alias),
            "Lambda": lambda: get_lambda_details(
                aws_client("lambda", region, sess), alias
            ),
            "Lightsail": lambda: get_lightsail_details(
                aws_client("lightsail", region, sess), alias
            ),
            "RDS": lambda: get_rds_details(
                aws_client("rds", region, sess), alias, sess
            ),
            "RDSReservedInstances": lambda: get_rds_reserved_instances(
                aws_client("rds", region, sess), alias, sess
            ),
            "S3": s3_collector,
            "VPN": lambda: get_vpn_details(aws_client("ec2", region, sess), alias),
            "WAFClassic": lambda: get_waf_classic_details(
                aws_client("waf-regional", region, sess), alias
            ),
            "WAFv2": lambda: get_waf_v2_details(
                aws_client("wafv2", region, sess), alias
            ),
        }
        collector_keys = set(collectors.keys())
        filtered = collector_keys
        if include_collectors:
            filtered = filtered & include_collectors
        if exclude_collectors:
            filtered = filtered - exclude_collectors
        collectors_to_run = {k: collectors[k] for k in filtered if k in collectors}
        region_block: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=MAX_TASKS_IN_REGION) as pool:
            futures = {pool.submit(fn): name for name, fn in collectors_to_run.items()}
            for fut in as_completed(futures):
                sheet = futures[fut]
                try:
                    region_block[sheet] = fut.result()
                except Exception as exc:
                    logger.error(
                        "Collector %s in %s failed: %s",
                        sheet,
                        region,
                        exc,
                        extra={"account": acct_id},
                    )
                    region_block[sheet] = []
        return region, region_block

    all_regions: dict[str, Any] = {}
    with ThreadPoolExecutor(
        max_workers=min(len(regions), MAX_REGIONS_IN_FLIGHT)
    ) as pool:
        for fut in as_completed(pool.submit(region_worker, r) for r in regions):
            region_name, svc_data = fut.result()
            all_regions[region_name] = svc_data

    all_regions["global"] = global_block
    return all_regions, alias


# ────────────────────────────── main CLI ─────────────────────────────
def parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AWS Inventory Exporter - Release Candidate"
    )
    parser.add_argument(
        "--master",
        action="store_true",
        help="Run from management / org-master account. Can be used with --exclude or --include.",
    )
    parser.add_argument(
        "--exclude-accounts",
        default="",
        help="Comma-separated list of account IDs to skip from scanning.",
    )
    parser.add_argument(
        "--include-accounts",
        default="",
        help="Comma-separated list of account IDs to exclusively scan.",
    )
    parser.add_argument(
        "--role-name",
        default=DEFAULT_ROLE,
        help="IAM role to assume in each member account. Default is 'OrganizationAccountAccessRole'.",
    )
    parser.add_argument(
        "--regions",
        help="Comma-separated list of regions to scan. Default is all regions.",
    )
    parser.add_argument(
        "--no-excel", action="store_true", help="Skip generation of Excel report."
    )
    parser.add_argument(
        "--include",
        default="",
        help="Comma-separated list of collectors to include. By default, all collectors are included. Can be used with --master.",
    )
    parser.add_argument(
        "--exclude",
        default="",
        help="Comma-separated list of collectors to exclude. Can be used with --master.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_cli()
    try:
        frozen = boto3.Session().get_credentials().get_frozen_credentials()
        if not (frozen.access_key and frozen.secret_key):
            raise RuntimeError
    except Exception:
        sys.exit(
            "ERROR: No AWS credentials - set AWS_PROFILE or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY"
        )

    base_session = boto3.Session()
    try:
        ec2 = base_session.client("ec2", config=RETRY_CONFIG)
        resp = ec2.describe_regions(
            Filters=[
                {"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}
            ]
        )
        enabled_regions = [r["RegionName"] for r in resp["Regions"]]
    except (ClientError, NoCredentialsError):
        enabled_regions = base_session.get_available_regions("ec2")

    regions = (
        [r.strip() for r in args.regions.split(",")]
        if args.regions
        else enabled_regions
    )
    unknown = set(regions) - set(enabled_regions)
    if unknown:
        logger.error(
            "Region(s) requested but not enabled in this account: %s",
            ", ".join(sorted(unknown)),
        )
        sys.exit(2)

    include_accounts = {
        a.strip() for a in args.include_accounts.split(",") if a.strip()
    }
    exclude_accounts = {
        a.strip() for a in args.exclude_accounts.split(",") if a.strip()
    }
    include_collectors = {c.strip() for c in args.include.split(",") if c.strip()}
    exclude_collectors = {c.strip() for c in args.exclude.split(",") if c.strip()}

    sessions: Dict[str, boto3.Session] = {}
    if args.master:
        org = base_session.client("organizations", config=RETRY_CONFIG)
        accounts = (
            a
            for page in org.get_paginator("list_accounts").paginate()
            for a in page["Accounts"]
            if a["Status"] == "ACTIVE"
        )
        for acct in accounts:
            aid = acct["Id"]
            if include_accounts and aid not in include_accounts:
                continue
            if aid in exclude_accounts:
                continue
            if sess := assume_role(aid, args.role_name, regions[0]):
                sessions[aid] = sess
    else:
        me = base_session.client("sts", config=RETRY_CONFIG).get_caller_identity()[
            "Account"
        ]
        if include_accounts and me not in include_accounts:
            return
        if me in exclude_accounts:
            return
        sessions[me] = base_session

    export_tz = str(get_localzone()) if get_localzone else "UTC"
    writer = None if args.no_excel else StreamingExcelWriter(EXCEL_FILENAME, export_tz)
    for acct_id, sess in sessions.items():
        log("info", f"Scanning account {acct_id}", account=acct_id)
        region_data, alias = scan_account(
            acct_id,
            sess,
            regions,
            include_collectors=include_collectors,
            exclude_collectors=exclude_collectors,
        )
        if writer:
            writer.record_account(acct_id, alias)
            for region_name, svc_block in region_data.items():
                for sheet, rows in svc_block.items():
                    for row in rows:
                        row["AccountId"] = acct_id
                        writer.write_row(sheet, row)
    if writer:
        writer.close()
    logger.info("Done.", extra={"account": "-"})


# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Interrupted.")
