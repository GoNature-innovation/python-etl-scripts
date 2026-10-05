"""
Google Sheets -> Azure Blob Storage (Parquet)
---------------------------------------------

Behavior
1. If the Azure Parquet blob does not exist:
   - Read every Google Sheet tab whose name contains "buyers" (case-insensitive)
   - Keep only: Name, Email Address, Mobile, City, Date
   - Consolidate all rows
   - Clean and standardize the data
   - Upload one Parquet file

2. If the Azure Parquet blob already exists:
   - Download the existing Parquet file
   - Derive the last processed source_row for every spreadsheet + sheet
   - Fetch only rows added after that source_row
   - Merge existing + new rows
   - Deduplicate using record_id
   - Overwrite the same Parquet file

Important
- This is an append-only incremental design.
- Changes made to old Google Sheet rows are not re-read automatically.
- No local checkpoint.json is required. The existing Parquet file is the checkpoint.
"""

import hashlib
import io
import logging
import os
import re
from typing import Dict, List, Tuple

import os
from dotenv import load_dotenv

load_dotenv()


import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, ContentSettings
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# BASE CONFIG
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SERVICE_ACCOUNT_FILE = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    os.path.join(BASE_DIR, "google-service-account.json"),
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

GOOGLE_END_COLUMN = "L"
ROW_COUNT_COLUMN = "A"


# ============================================================
# GOOGLE SPREADSHEETS
# ============================================================

SPREADSHEETS = [
    "10yMjGBw9eXGkS4hkNyx2epfjNFB0XWVaqk5qvy9ZMQE",
    "1aikawrZkrUa1ZBHD-1Qxb-Ksgyrf-SjlpPTMwiNpbpk",
    
]


# ============================================================
# AZURE CONFIG
# ============================================================

# Recommended:
# Store the connection string in an environment variable instead
# of keeping the storage key inside this Python file.
#
# Linux:
# export AZURE_STORAGE_CONNECTION_STRING="..."
#
# Docker:
# docker run --env-file .env ...

AZURE_STORAGE_CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING"
)

AZURE_STORAGE_CONTAINER = os.getenv(
    "AZURE_STORAGE_CONTAINER",
    "raw-data"
)

AZURE_BLOB_NAME = os.getenv(
    "AZURE_BLOB_NAME",
    "razorpay_Silver_p3599_src.parquet"
)


# ============================================================
# FINAL DATASET SCHEMA
# ============================================================

# Only these sheet columns are ingested (normalized from
# "Name", "Email Address", "Mobile", "City", "Date").
BUSINESS_COLUMNS = [
    "name",
    "email_address",
    "mobile",
    "city",
    "date",
]

FINAL_COLUMNS = BUSINESS_COLUMNS + [
    "source_workbook",
    "source_spreadsheet",
    "source_sheet",
    "source_row",
    "record_id",
    "row_hash",
    "ingested_at_utc",
]

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string()),
        pa.field("email_address", pa.string()),
        pa.field("mobile", pa.string()),
        pa.field("city", pa.string()),
        pa.field("date", pa.string()),
        pa.field("source_workbook", pa.string()),
        pa.field("source_spreadsheet", pa.string()),
        pa.field("source_sheet", pa.string()),
        pa.field("source_row", pa.int64()),
        pa.field("record_id", pa.string()),
        pa.field("row_hash", pa.string()),
        pa.field("ingested_at_utc", pa.string()),
    ]
)


# ============================================================
# GENERIC HELPERS
# ============================================================

def normalize_headers(headers: List[str]) -> List[str]:
    """Convert Google Sheet headers to unique snake_case names."""

    normalized = []
    seen = {}

    for position, header in enumerate(headers):
        name = re.sub(
            r"[^a-zA-Z0-9]+",
            "_",
            str(header).strip().lower(),
        ).strip("_")

        if not name:
            name = f"column_{position + 1}"

        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1

        normalized.append(name)

    return normalized


def safe_nullable_integer(series: pd.Series) -> pd.Series:
    """Convert only whole-number values to nullable pandas Int64."""

    numeric = pd.to_numeric(series, errors="coerce")

    whole_number_mask = numeric.isna() | (numeric % 1 == 0)
    numeric = numeric.where(whole_number_mask)

    return numeric.astype("Int64")


def normalize_datetime(series: pd.Series) -> pd.Series:
    """
    Convert supported date representations to datetime64[ns].

    Handles:
    - datetime values
    - Unix nanoseconds
    - Unix microseconds
    - Unix milliseconds
    - Unix seconds
    - Google Sheets serial dates
    - text dates
    """

    if pd.api.types.is_datetime64_any_dtype(series):
        result = pd.to_datetime(series, errors="coerce")
        if getattr(result.dtype, "tz", None) is not None:
            result = result.dt.tz_localize(None)
        return result

    raw = series.copy()
    numeric = pd.to_numeric(raw, errors="coerce")

    result = pd.Series(
        pd.NaT,
        index=series.index,
        dtype="datetime64[ns]",
    )

    masks = [
        (
            numeric.notna() & (numeric.abs() >= 10**17),
            "ns",
        ),
        (
            numeric.notna()
            & (numeric.abs() >= 10**14)
            & (numeric.abs() < 10**17),
            "us",
        ),
        (
            numeric.notna()
            & (numeric.abs() >= 10**11)
            & (numeric.abs() < 10**14),
            "ms",
        ),
        (
            numeric.notna()
            & (numeric.abs() >= 10**8)
            & (numeric.abs() < 10**11),
            "s",
        ),
    ]

    for mask, unit in masks:
        if mask.any():
            result.loc[mask] = pd.to_datetime(
                numeric.loc[mask],
                unit=unit,
                errors="coerce",
            )

    serial_mask = numeric.notna() & (numeric.abs() < 10**8)

    if serial_mask.any():
        result.loc[serial_mask] = pd.to_datetime(
            numeric.loc[serial_mask],
            unit="D",
            origin="1899-12-30",
            errors="coerce",
        ).dt.round("s")

    text_mask = numeric.isna()

    if text_mask.any():
        result.loc[text_mask] = pd.to_datetime(
            raw.loc[text_mask],
            format="mixed",
            dayfirst=True,
            errors="coerce",
        )

    return result


def datetime_to_sql_string(series: pd.Series) -> pd.Series:
    """Convert datetime-like values to SQL-friendly UTF8 strings."""

    values = normalize_datetime(series)

    result = values.dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    result = result.where(values.notna(), None)

    return result.astype("string")


# ============================================================
# GOOGLE CONNECTION
# ============================================================

def get_google_service():
    """Create authenticated Google Sheets API client."""

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            f"Google service account file not found: {SERVICE_ACCOUNT_FILE}"
        )

    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )

    return build(
        "sheets",
        "v4",
        credentials=credentials,
        cache_discovery=False,
    )


def get_workbook_name(service, spreadsheet_id: str) -> str:
    """Return Google spreadsheet title."""

    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="properties.title",
        )
        .execute()
    )

    return (
        metadata.get("properties", {})
        .get("title", spreadsheet_id)
    )


def get_buyer_sheet_names(service, spreadsheet_id: str) -> List[str]:
    """Return only tab names containing 'buyers' (case-insensitive)."""

    try:
        metadata = (
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets.properties.title",
            )
            .execute()
        )
    except HttpError as error:
        raise RuntimeError(
            f"Unable to read spreadsheet {spreadsheet_id}: {error}"
        ) from error

    all_sheet_names = [
        sheet["properties"]["title"]
        for sheet in metadata.get("sheets", [])
    ]

    return [
        sheet_name
        for sheet_name in all_sheet_names
        if "buyers" in sheet_name.lower()
    ]


def get_current_sheet_rows(
    service,
    spreadsheet_id: str,
    sheet_names: List[str],
) -> Dict[str, int]:
    """Return current used row count from column A for every sheet."""

    if not sheet_names:
        return {}

    ranges = [
        f"'{sheet_name}'!{ROW_COUNT_COLUMN}:{ROW_COUNT_COLUMN}"
        for sheet_name in sheet_names
    ]

    response = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )

    return {
        sheet_name: len(value_range.get("values", []))
        for sheet_name, value_range in zip(
            sheet_names,
            response.get("valueRanges", []),
        )
    }


def get_sheet_headers(
    service,
    spreadsheet_id: str,
    sheet_names: List[str],
) -> Dict[str, List[str]]:
    """Return normalized row-1 headers for every sheet."""

    if not sheet_names:
        return {}

    ranges = [
        f"'{sheet_name}'!A1:{GOOGLE_END_COLUMN}1"
        for sheet_name in sheet_names
    ]

    response = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )

    result = {}

    for sheet_name, value_range in zip(
        sheet_names,
        response.get("valueRanges", []),
    ):
        values = value_range.get("values", [])

        if values:
            result[sheet_name] = normalize_headers(values[0])

    return result


# ============================================================
# ROW / DATAFRAME BUILDING
# ============================================================

def create_dataframe_from_rows(
    rows: List[List],
    headers: List[str],
    workbook_name: str,
    spreadsheet_id: str,
    sheet_name: str,
    start_row: int,
) -> pd.DataFrame:
    """Convert Google API row arrays into a DataFrame."""

    normalized_rows = []

    for row in rows:
        row = row[: len(headers)]

        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        normalized_rows.append(row)

    if not normalized_rows:
        return pd.DataFrame()

    dataframe = pd.DataFrame(
        normalized_rows,
        columns=headers,
    )

    dataframe["source_workbook"] = workbook_name
    dataframe["source_spreadsheet"] = spreadsheet_id
    dataframe["source_sheet"] = sheet_name
    dataframe["source_row"] = range(
        start_row,
        start_row + len(dataframe),
    )

    return dataframe


def create_record_id(row: pd.Series) -> str:
    """
    Stable logical record identifier.

    Current identity:
    email + mobile + date
    """

    email = str(row.get("email_address", "")).strip().lower()
    mobile = str(row.get("mobile", "")).strip()
    date = row.get("date")

    if pd.isna(date):
        date_value = ""
    else:
        date_value = (
            pd.Timestamp(date)
            .strftime("%Y-%m-%d %H:%M:%S")
        )

    raw_key = f"{email}|{mobile}|{date_value}"

    return hashlib.sha256(
        raw_key.encode("utf-8")
    ).hexdigest()


def create_row_hash(row: pd.Series) -> str:
    """Hash the business/source values to detect row-content differences."""

    excluded_columns = {
        "record_id",
        "row_hash",
        "ingested_at_utc",
    }

    values = []

    for column in sorted(row.index):
        if column in excluded_columns:
            continue

        value = row.get(column)

        if pd.isna(value):
            value = ""
        elif isinstance(value, pd.Timestamp):
            value = value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            value = str(value).strip()

        values.append(f"{column}={value}")

    raw_value = "|".join(values)

    return hashlib.sha256(
        raw_value.encode("utf-8")
    ).hexdigest()


def prepare_final_dataframe(
    dataframe: pd.DataFrame,
    regenerate_metadata: bool = True,
) -> pd.DataFrame:
    """Clean, type, hash and enforce the final dataset schema."""

    if dataframe.empty:
        return dataframe

    df = dataframe.copy()

    # Keep only the screenshot columns + source/metadata columns.
    df = df[
        [
            column
            for column in df.columns
            if column in FINAL_COLUMNS
        ]
    ]

    df = df.replace(
        r"^\s*$",
        pd.NA,
        regex=True,
    )

    for column in FINAL_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    df = df.dropna(
        how="all",
        subset=BUSINESS_COLUMNS,
    )

    df["date"] = normalize_datetime(df["date"])

    df["source_row"] = safe_nullable_integer(df["source_row"])

    string_columns = [
        "name",
        "email_address",
        "mobile",
        "city",
        "source_workbook",
        "source_spreadsheet",
        "source_sheet",
    ]

    for column in string_columns:
        df[column] = (
            df[column]
            .astype("string")
            .str.strip()
        )

    if regenerate_metadata:
        df["record_id"] = df.apply(
            create_record_id,
            axis=1,
        )

        df["row_hash"] = df.apply(
            create_row_hash,
            axis=1,
        )

        df["ingested_at_utc"] = (
            pd.Timestamp.now(tz="UTC")
            .tz_localize(None)
        )
    else:
        # Existing Parquet stores these as strings.
        # Convert ingestion time back to datetime for validation/processing.
        df["ingested_at_utc"] = normalize_datetime(
            df["ingested_at_utc"]
        )

    df = df.reindex(columns=FINAL_COLUMNS)

    df = (
        df.drop_duplicates(
            subset=["record_id"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    return df


# ============================================================
# AZURE STORAGE
# ============================================================

def get_blob_service_client() -> BlobServiceClient:
    """Create Azure BlobServiceClient."""

    if not AZURE_STORAGE_CONNECTION_STRING:
        raise ValueError(
            "AZURE_STORAGE_CONNECTION_STRING is missing. "
            "Set it as an environment variable."
        )

    return BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )


def get_blob_client():
    """Return configured blob client."""

    service_client = get_blob_service_client()

    return service_client.get_blob_client(
        container=AZURE_STORAGE_CONTAINER,
        blob=AZURE_BLOB_NAME,
    )


def blob_exists() -> bool:
    """Check whether the target Parquet blob exists."""

    exists = get_blob_client().exists()

    logger.info(
        "Azure blob exists: %s",
        exists,
    )

    return exists


def download_dataframe() -> pd.DataFrame:
    """Download existing Azure Parquet file into pandas."""

    blob_client = get_blob_client()

    if not blob_client.exists():
        raise FileNotFoundError(
            f"Azure blob does not exist: "
            f"{AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_NAME}"
        )

    logger.info("Downloading existing Parquet from Azure...")

    parquet_bytes = (
        blob_client.download_blob().readall()
    )

    dataframe = pd.read_parquet(
        io.BytesIO(parquet_bytes),
        engine="pyarrow",
    )

    logger.info(
        "Downloaded %d existing rows",
        len(dataframe),
    )

    return dataframe


def prepare_for_parquet(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Convert pandas values to the exact Parquet output types."""

    if dataframe.empty:
        raise ValueError(
            "Cannot prepare an empty DataFrame."
        )

    df = dataframe.copy()

    for column in FINAL_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    df["source_row"] = safe_nullable_integer(df["source_row"])

    df["date"] = datetime_to_sql_string(df["date"])

    df["ingested_at_utc"] = datetime_to_sql_string(
        df["ingested_at_utc"]
    )

    string_columns = [
        "name",
        "email_address",
        "mobile",
        "city",
        "source_workbook",
        "source_spreadsheet",
        "source_sheet",
        "record_id",
        "row_hash",
    ]

    for column in string_columns:
        df[column] = (
            df[column]
            .astype("string")
            .str.strip()
        )

    return df.reindex(columns=FINAL_COLUMNS)


def upload_dataframe(
    dataframe: pd.DataFrame,
) -> str:
    """Serialize DataFrame to Snappy Parquet and overwrite Azure blob."""

    if dataframe.empty:
        raise ValueError(
            "DataFrame is empty. Azure upload cancelled."
        )

    prepared = prepare_for_parquet(dataframe)

    table = pa.Table.from_pandas(
        prepared,
        schema=PARQUET_SCHEMA,
        preserve_index=False,
        safe=False,
    )

    buffer = io.BytesIO()

    pq.write_table(
        table,
        buffer,
        compression="snappy",
    )

    size_bytes = buffer.tell()
    buffer.seek(0)

    blob_client = get_blob_client()

    logger.info(
        "Uploading %d rows to Azure...",
        len(prepared),
    )

    blob_client.upload_blob(
        data=buffer,
        overwrite=True,
        content_settings=ContentSettings(
            content_type="application/vnd.apache.parquet"
        ),
    )

    logger.info(
        "Upload complete: %s/%s | %.2f MB",
        AZURE_STORAGE_CONTAINER,
        AZURE_BLOB_NAME,
        size_bytes / 1024 / 1024,
    )

    return (
        f"{AZURE_STORAGE_CONTAINER}/"
        f"{AZURE_BLOB_NAME}"
    )


# ============================================================
# FULL LOAD
# ============================================================

def extract_full_spreadsheet(
    service,
    spreadsheet_id: str,
) -> pd.DataFrame:
    """Extract every row from every Buyers tab in one workbook."""

    workbook_name = get_workbook_name(
        service,
        spreadsheet_id,
    )

    sheet_names = get_buyer_sheet_names(
        service,
        spreadsheet_id,
    )

    logger.info(
        "Full load workbook: %s",
        workbook_name,
    )

    if not sheet_names:
        logger.info("No Buyers tabs found.")
        return pd.DataFrame()

    ranges = [
        f"'{sheet_name}'!A:{GOOGLE_END_COLUMN}"
        for sheet_name in sheet_names
    ]

    response = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )

    dataframes = []

    for sheet_name, value_range in zip(
        sheet_names,
        response.get("valueRanges", []),
    ):
        rows = value_range.get("values", [])

        if len(rows) <= 1:
            logger.info(
                "%s: no data rows",
                sheet_name,
            )
            continue

        headers = normalize_headers(rows[0])

        dataframe = create_dataframe_from_rows(
            rows=rows[1:],
            headers=headers,
            workbook_name=workbook_name,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            start_row=2,
        )

        if not dataframe.empty:
            dataframes.append(dataframe)

        logger.info(
            "%s: %d rows",
            sheet_name,
            len(dataframe),
        )

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def extract_full_load(service) -> pd.DataFrame:
    """Extract and combine all configured Google workbooks."""

    all_dataframes = []

    for spreadsheet_id in SPREADSHEETS:
        dataframe = extract_full_spreadsheet(
            service,
            spreadsheet_id,
        )

        if not dataframe.empty:
            all_dataframes.append(dataframe)

    if not all_dataframes:
        raise ValueError(
            "No buyer records found in configured spreadsheets."
        )

    dataframe = pd.concat(
        all_dataframes,
        ignore_index=True,
    )

    return prepare_final_dataframe(
        dataframe,
        regenerate_metadata=True,
    )


# ============================================================
# INCREMENTAL LOAD
# ============================================================

def build_checkpoint_from_dataframe(
    dataframe: pd.DataFrame,
) -> Dict[str, Dict[str, int]]:
    """
    Derive latest processed source_row for every spreadsheet + sheet.

    The existing Azure Parquet file acts as the checkpoint.
    """

    required_columns = {
        "source_spreadsheet",
        "source_sheet",
        "source_row",
    }

    if not required_columns.issubset(
        dataframe.columns
    ):
        raise ValueError(
            "Existing Parquet does not contain the "
            "source columns required for incremental loading."
        )

    temp = dataframe[
        [
            "source_spreadsheet",
            "source_sheet",
            "source_row",
        ]
    ].copy()

    temp["source_row"] = pd.to_numeric(
        temp["source_row"],
        errors="coerce",
    )

    temp = temp.dropna(
        subset=[
            "source_spreadsheet",
            "source_sheet",
            "source_row",
        ]
    )

    checkpoint: Dict[str, Dict[str, int]] = {}

    grouped = (
        temp.groupby(
            [
                "source_spreadsheet",
                "source_sheet",
            ]
        )["source_row"]
        .max()
    )

    for (
        spreadsheet_id,
        sheet_name,
    ), source_row in grouped.items():

        spreadsheet_id = str(spreadsheet_id)
        sheet_name = str(sheet_name)

        checkpoint.setdefault(
            spreadsheet_id,
            {},
        )[sheet_name] = int(source_row)

    return checkpoint


def extract_incremental_spreadsheet(
    service,
    spreadsheet_id: str,
    spreadsheet_checkpoint: Dict[str, int],
) -> pd.DataFrame:
    """Fetch only rows after each sheet's last processed source_row."""

    workbook_name = get_workbook_name(
        service,
        spreadsheet_id,
    )

    sheet_names = get_buyer_sheet_names(
        service,
        spreadsheet_id,
    )

    logger.info(
        "Checking workbook: %s",
        workbook_name,
    )

    if not sheet_names:
        return pd.DataFrame()

    current_rows = get_current_sheet_rows(
        service,
        spreadsheet_id,
        sheet_names,
    )

    headers = get_sheet_headers(
        service,
        spreadsheet_id,
        sheet_names,
    )

    ranges = []
    range_info = []

    for sheet_name in sheet_names:

        current_last_row = int(
            current_rows.get(
                sheet_name,
                0,
            )
        )

        # New sheet => checkpoint defaults to header row.
        last_processed_row = int(
            spreadsheet_checkpoint.get(
                sheet_name,
                1,
            )
        )

        if current_last_row < last_processed_row:
            logger.warning(
                "%s shrank from checkpoint row %d to %d. "
                "No automatic historical reconciliation is performed.",
                sheet_name,
                last_processed_row,
                current_last_row,
            )
            continue

        if current_last_row == last_processed_row:
            logger.info(
                "No new rows: %s",
                sheet_name,
            )
            continue

        start_row = last_processed_row + 1

        logger.info(
            "New rows: %s [%d-%d]",
            sheet_name,
            start_row,
            current_last_row,
        )

        ranges.append(
            f"'{sheet_name}'!"
            f"A{start_row}:"
            f"{GOOGLE_END_COLUMN}"
            f"{current_last_row}"
        )

        range_info.append(
            {
                "sheet_name": sheet_name,
                "start_row": start_row,
            }
        )

    if not ranges:
        return pd.DataFrame()

    response = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )

    dataframes = []

    for info, value_range in zip(
        range_info,
        response.get("valueRanges", []),
    ):
        sheet_name = info["sheet_name"]
        start_row = info["start_row"]

        rows = value_range.get(
            "values",
            [],
        )

        if not rows:
            continue

        sheet_headers = headers.get(
            sheet_name,
            [],
        )

        if not sheet_headers:
            logger.warning(
                "No header found for %s",
                sheet_name,
            )
            continue

        dataframe = create_dataframe_from_rows(
            rows=rows,
            headers=sheet_headers,
            workbook_name=workbook_name,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            start_row=start_row,
        )

        if not dataframe.empty:
            dataframes.append(dataframe)

    if not dataframes:
        return pd.DataFrame()

    dataframe = pd.concat(
        dataframes,
        ignore_index=True,
    )

    return prepare_final_dataframe(
        dataframe,
        regenerate_metadata=True,
    )


def extract_incremental_load(
    service,
    checkpoint: Dict[str, Dict[str, int]],
) -> pd.DataFrame:
    """Fetch incremental rows from all configured spreadsheets."""

    all_dataframes = []

    for spreadsheet_id in SPREADSHEETS:

        spreadsheet_checkpoint = checkpoint.get(
            spreadsheet_id,
            {},
        )

        dataframe = extract_incremental_spreadsheet(
            service,
            spreadsheet_id,
            spreadsheet_checkpoint,
        )

        if not dataframe.empty:
            all_dataframes.append(dataframe)

    if not all_dataframes:
        return pd.DataFrame()

    dataframe = pd.concat(
        all_dataframes,
        ignore_index=True,
    )

    return prepare_final_dataframe(
        dataframe,
        regenerate_metadata=True,
    )


# ============================================================
# VALIDATION
# ============================================================

def validate_dataframe(
    dataframe: pd.DataFrame,
    dataframe_name: str,
) -> None:
    """Basic safety checks before Azure upload."""

    if dataframe.empty:
        raise ValueError(
            f"{dataframe_name} is empty."
        )

    missing_columns = [
        column
        for column in FINAL_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{dataframe_name} missing columns: "
            f"{missing_columns}"
        )

    if not pd.api.types.is_datetime64_any_dtype(
        dataframe["date"]
    ):
        raise TypeError(
            "date must be datetime before Parquet serialization."
        )

    if not pd.api.types.is_datetime64_any_dtype(
        dataframe["ingested_at_utc"]
    ):
        raise TypeError(
            "ingested_at_utc must be datetime before Parquet serialization."
        )

    if dataframe["record_id"].isna().any():
        raise ValueError(
            "record_id contains null values."
        )

    logger.info(
        "%s validated successfully: %d rows",
        dataframe_name,
        len(dataframe),
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def main() -> None:
    """Run full or incremental Google Sheets -> Azure pipeline."""

    logger.info(
        "============================================="
    )
    logger.info(
        "GOOGLE SHEETS -> AZURE PARQUET PIPELINE"
    )
    logger.info(
        "============================================="
    )

    if not SPREADSHEETS:
        raise ValueError(
            "No Google spreadsheets configured."
        )

    service = get_google_service()

    # --------------------------------------------------------
    # CASE 1: PARQUET DOES NOT EXIST -> FULL LOAD
    # --------------------------------------------------------

    if not blob_exists():

        logger.info(
            "Azure Parquet not found -> FULL LOAD"
        )

        full_df = extract_full_load(service)

        validate_dataframe(
            full_df,
            "FULL DATAFRAME",
        )

        upload_dataframe(full_df)

        logger.info(
            "FULL LOAD COMPLETED SUCCESSFULLY"
        )

        return

    # --------------------------------------------------------
    # CASE 2: PARQUET EXISTS -> INCREMENTAL LOAD
    # --------------------------------------------------------

    logger.info(
        "Azure Parquet exists -> INCREMENTAL LOAD"
    )

    existing_df = download_dataframe()

    # Preserve existing IDs/hashes/timestamps rather than regenerating them.
    existing_df = prepare_final_dataframe(
        existing_df,
        regenerate_metadata=False,
    )

    checkpoint = build_checkpoint_from_dataframe(
        existing_df
    )

    incremental_df = extract_incremental_load(
        service,
        checkpoint,
    )

    if incremental_df.empty:

        logger.info(
            "No new rows found. Azure blob was not modified."
        )

        return

    logger.info(
        "New incremental rows: %d",
        len(incremental_df),
    )

    combined_df = pd.concat(
        [
            existing_df,
            incremental_df,
        ],
        ignore_index=True,
    )

    # Do not regenerate metadata for old rows.
    combined_df = (
        combined_df
        .drop_duplicates(
            subset=["record_id"],
            keep="last",
        )
        .reindex(columns=FINAL_COLUMNS)
        .reset_index(drop=True)
    )

    validate_dataframe(
        combined_df,
        "COMBINED DATAFRAME",
    )

    upload_dataframe(combined_df)

    logger.info(
        "INCREMENTAL LOAD COMPLETED SUCCESSFULLY"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception(
            "PIPELINE FAILED"
        )
        raise
