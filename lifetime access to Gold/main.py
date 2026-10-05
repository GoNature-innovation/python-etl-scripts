"""
Lifetime Access to Gold - Google Sheet client extractor
-------------------------------------------------------

The tracker tab is not a clean table. A single tab holds several blocks
(main list, "GH REPLAY IN PARTS", "REFUNDS", "DEACTIVATED", ...), and each
block can start in a different column (NAMES in column A in one block,
column C or D in another).

What this script does
1. Reads the tab(s) you choose from the spreadsheet.
2. Scans every row and detects header rows by their text (a row that has a
   NAME header and an EMAIL header), so each block is read with its own
   column positions.
3. Section title rows (e.g. "REFUNDS", "DEACTIVATED") are remembered and
   attached to every client row below them.
4. Every client row is mapped onto one fixed (symmetric) set of columns.
5. Checkbox columns (Access Gold Pro, Access Silver Lifetime,
   Access Gold Pro Lifetime, and any other tick column) become True / False.
6. client_status is derived:
      Deactivated -> status tag or section says DEACTIVATED
      Refund      -> status tag or section says REFUND
      Active      -> everything else
7. Writes an Excel (.xlsx) file to the output folder.

Usage
    python main.py --list                       # show tab names
    python main.py                              # pick tab(s) interactively
    python main.py --sheet "MASTER LIST"        # one tab
    python main.py --sheet "Tab A" --sheet "Tab B"
    python main.py --all                        # every tab, combined

The spreadsheet ID (or full URL) comes from --spreadsheet or SPREADSHEET_ID
in .env.
"""

import argparse
import logging
import os
import re
import sys
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()


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

# Blank values in .env fall back to the defaults; relative paths are
# resolved from this script's folder.
SERVICE_ACCOUNT_FILE = os.path.join(
    BASE_DIR,
    os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or "google-service-account.json",
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")

OUTPUT_DIR = os.path.join(BASE_DIR, os.getenv("OUTPUT_DIR") or "output")


# ============================================================
# FINAL DATASET SCHEMA
# ============================================================

ACCESS_COLUMNS = [
    "access_gold_pro",
    "access_silver_lifetime",
    "access_gold_pro_lifetime",
]

# Always present, always in this order. Any other header found in the
# sheet (payments, Agra event, etc.) is appended after these.
CORE_COLUMNS = [
    "name",
    "email",
    "mobile",
    "coach",
    "tech_member",
    "status",
    "status_data",
    "client_status",
    "is_active",
    *ACCESS_COLUMNS,
    "comment",
]

META_COLUMNS = [
    "section",
    "source_sheet",
    "source_row",
]

TRUE_STRINGS = {"true", "yes", "y", "1", "✓", "✔", "☑"}
FALSE_STRINGS = {"false", "no", "n", "0", "☐"}
BOOL_STRINGS = TRUE_STRINGS | FALSE_STRINGS


# ============================================================
# TEXT HELPERS
# ============================================================

def clean_text(value) -> str:
    """Trim and collapse whitespace."""

    if value is None:
        return ""

    return re.sub(r"\s+", " ", str(value)).strip()


def header_key(text: str) -> str:
    """'PH NO.' -> 'ph no', 'ACCESS - GOLD' -> 'access gold'."""

    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def snake_case(text: str) -> str:
    return header_key(text).replace(" ", "_")


def is_bool_text(value: str) -> bool:
    return value.lower() in BOOL_STRINGS


def to_bool(value) -> bool:
    """Ticked checkbox -> True, unticked / blank -> False."""

    if isinstance(value, bool):
        return value

    return clean_text(value).lower() in TRUE_STRINGS


def extract_spreadsheet_id(value: str) -> str:
    """Accept either a raw spreadsheet ID or a full Google Sheets URL."""

    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    return match.group(1) if match else value.strip()


# ============================================================
# HEADER DETECTION
# ============================================================

def canonical_header(text: str) -> Optional[str]:
    """Map a raw header cell to one of the fixed output columns."""

    key = header_key(text)
    if not key:
        return None

    words = set(key.split())

    if "status" in words and "data" in words:
        return "status_data"
    if "status" in words:
        return "status"
    if "email" in key or "mail" in words:
        return "email"
    if (
        key.startswith("ph")
        or "phone" in key
        or "mobile" in key
        or "contact" in words
        or "whatsapp" in key
    ):
        return "mobile"
    if "coach" in key:
        return "coach"
    if "tech" in words:
        return "tech_member"
    if "comment" in key or "remark" in key:
        return "comment"
    if "silver" in words:
        return "access_silver_lifetime"
    if "gold" in words:
        if "lifetime" in key or "life time" in key:
            return "access_gold_pro_lifetime"
        return "access_gold_pro"
    if "name" in words or "names" in words:
        return "name"

    return None


def build_header_layout(
    row: List[str],
    ignore_values: set,
) -> Dict[int, str]:
    """
    Return {column_index: output_column_name} for a header row.

    Cells equal to the current section title (e.g. "REFUNDS" repeated across
    the refund header row) are ignored so they don't become columns.
    """

    layout: Dict[int, str] = {}
    used: Counter = Counter()

    for index, cell in enumerate(row):
        if not cell or cell.upper() in ignore_values:
            continue

        name = canonical_header(cell)

        # Two gold headers without "lifetime" in the text: the second one
        # is the lifetime column (sheet order: Gold Pro, Silver, Gold Lifetime).
        if name == "access_gold_pro" and used[name]:
            name = "access_gold_pro_lifetime"

        if name is None or used[name]:
            base = name or snake_case(cell)
            used[base] += 1
            name = base if used[base] == 1 else f"{base}_{used[base]}"
        else:
            used[name] += 1

        layout[index] = name

    return layout


def is_header_row(row: List[str]) -> bool:
    names = {canonical_header(cell) for cell in row if cell}
    return "name" in names and "email" in names


def merge_with_previous_layout(
    new_layout: Dict[int, str],
    previous_layout: Optional[Dict[int, str]],
) -> Dict[int, str]:
    """
    Later blocks (REFUNDS, DEACTIVATED) often leave the checkbox columns
    without a header. Re-use the column position from the previous header
    when that position is unused and the column is missing in this block.
    """

    if not previous_layout:
        return new_layout

    merged = dict(new_layout)
    present = set(new_layout.values())

    for index, name in previous_layout.items():
        if index not in merged and name not in present:
            merged[index] = name
            present.add(name)

    return merged


# ============================================================
# ROW CLASSIFICATION
# ============================================================

def section_title(row: List[str]) -> Optional[str]:
    """A row with one repeated, upper-case text value is a section title."""

    texts = [cell for cell in row if cell and not is_bool_text(cell)]
    if not texts:
        return None

    distinct = {text.upper() for text in texts}
    title = texts[0]

    if len(distinct) == 1 and title == title.upper() and re.search(r"[A-Z]", title):
        return title

    return None


def is_client_row(record: Dict[str, str], row: List[str]) -> bool:
    if "@" in record.get("email", ""):
        return True

    if len(re.sub(r"\D", "", record.get("mobile", ""))) >= 7:
        return True

    if record.get("name"):
        distinct = {
            cell.upper() for cell in row if cell and not is_bool_text(cell)
        }
        return len(distinct) >= 2

    return False


# ============================================================
# SHEET PARSING
# ============================================================

def parse_sheet(sheet_name: str, values: List[List]) -> List[Dict]:
    """Turn one irregular tab into a list of uniform client records."""

    records: List[Dict] = []
    layout: Optional[Dict[int, str]] = None
    section = "MAIN"

    for row_number, raw_row in enumerate(values, start=1):
        row = [clean_text(cell) for cell in raw_row]

        if not any(row):
            continue

        if is_header_row(row):
            new_layout = build_header_layout(row, {section.upper()})
            layout = merge_with_previous_layout(new_layout, layout)
            logger.info(
                "[%s] header at row %s (section %s): %s",
                sheet_name,
                row_number,
                section,
                ", ".join(f"{name}@{index + 1}" for index, name in sorted(layout.items())),
            )
            continue

        record = {}
        if layout:
            record = {
                name: row[index] if index < len(row) else ""
                for index, name in layout.items()
            }

        if layout and is_client_row(record, row):
            record["section"] = section
            record["source_sheet"] = sheet_name
            record["source_row"] = row_number
            records.append(record)
            continue

        title = section_title(row)
        if title:
            section = title

    logger.info("[%s] %s client rows", sheet_name, len(records))
    return records


# ============================================================
# CLEANING
# ============================================================

def derive_client_status(status: str, section: str) -> str:
    text = f"{status} {section}".upper()

    if "DEACTIVAT" in text:
        return "Deactivated"
    if "REFUND" in text:
        return "Refund"
    return "Active"


def build_dataframe(records: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)

    for column in CORE_COLUMNS + META_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    df = df.fillna("")

    df["name"] = df["name"].str.title()
    df["email"] = df["email"].str.lower().str.replace(" ", "", regex=False)
    df["mobile"] = df["mobile"].astype(str).str.replace(r"[^\d+]", "", regex=True)

    df["client_status"] = [
        derive_client_status(status, section)
        for status, section in zip(df["status"], df["section"])
    ]
    df["is_active"] = df["client_status"].eq("Active")

    # Named access columns are always boolean.
    for column in ACCESS_COLUMNS:
        df[column] = df[column].map(to_bool)

    # Any other column that only holds TRUE/FALSE (e.g. Agra event tick).
    for column in df.columns:
        if column in ACCESS_COLUMNS or column == "is_active":
            continue
        non_empty = df[column].astype(str).str.strip()
        non_empty = non_empty[non_empty != ""]
        if len(non_empty) and non_empty.str.lower().isin(BOOL_STRINGS).all():
            df[column] = df[column].map(to_bool)

    extra_columns = [
        column
        for column in df.columns
        if column not in CORE_COLUMNS and column not in META_COLUMNS
    ]

    df = df[CORE_COLUMNS + extra_columns + META_COLUMNS]
    df["source_row"] = df["source_row"].astype("Int64")

    return df.reset_index(drop=True)


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


def get_sheet_names(service, spreadsheet_id: str) -> List[str]:
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
        if error.resp.status == 403:
            raise SystemExit(
                f"\nNo permission to open spreadsheet {spreadsheet_id}.\n"
                f"Share the Google Sheet (Viewer) with: "
                f"{service._http.credentials.service_account_email}\n"
            ) from None
        if error.resp.status == 404:
            raise SystemExit(
                f"\nSpreadsheet {spreadsheet_id} not found. Check SPREADSHEET_ID in .env.\n"
            ) from None
        raise RuntimeError(
            f"Unable to read spreadsheet {spreadsheet_id}: {error}"
        ) from error

    return [
        sheet["properties"]["title"]
        for sheet in metadata.get("sheets", [])
    ]


def read_sheet_values(service, spreadsheet_id: str, sheet_name: str) -> List[List]:
    """
    FORMATTED_VALUE keeps phone numbers as typed (no 9.19E+11) and returns
    checkboxes as "TRUE" / "FALSE".
    """

    response = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'",
            majorDimension="ROWS",
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
    )

    return response.get("values", [])


# ============================================================
# SHEET SELECTION
# ============================================================

def choose_sheets_interactively(sheet_names: List[str]) -> List[str]:
    print("\nAvailable tabs:")
    for number, name in enumerate(sheet_names, start=1):
        print(f"  {number:>2}. {name}")

    answer = input("\nEnter tab number(s), comma separated (or 'all'): ").strip()

    if answer.lower() == "all":
        return sheet_names

    chosen = []
    for part in answer.split(","):
        part = part.strip()
        if part.isdigit() and 1 <= int(part) <= len(sheet_names):
            chosen.append(sheet_names[int(part) - 1])
        elif part:
            raise ValueError(f"Invalid tab number: {part}")

    return chosen


def resolve_sheets(requested: List[str], available: List[str]) -> List[str]:
    """Match requested tab names case-insensitively."""

    lookup = {name.strip().lower(): name for name in available}
    resolved = []

    for name in requested:
        match = lookup.get(name.strip().lower())
        if not match:
            raise ValueError(
                f"Tab '{name}' not found. Available: {', '.join(available)}"
            )
        resolved.append(match)

    return resolved


# ============================================================
# OUTPUT
# ============================================================

def save_output(df: pd.DataFrame, sheet_names: List[str]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    label = snake_case(sheet_names[0]) if len(sheet_names) == 1 else "combined"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUTPUT_DIR, f"clients_{label}_{stamp}.xlsx")

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Clients", index=False)

        worksheet = writer.sheets["Clients"]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions

        for column_cells in worksheet.columns:
            width = max(len(str(cell.value or "")) for cell in column_cells)
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(width + 2, 50)

    logger.info("Saved %s", path)


# ============================================================
# MAIN
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract clients from the Lifetime Access to Gold tracker.",
    )
    parser.add_argument(
        "--spreadsheet",
        default=SPREADSHEET_ID,
        help="Spreadsheet ID or URL (default: SPREADSHEET_ID from .env)",
    )
    parser.add_argument(
        "--sheet",
        action="append",
        default=[],
        help="Tab name to read. Repeat for several tabs.",
    )
    parser.add_argument("--all", action="store_true", help="Read every tab.")
    parser.add_argument("--list", action="store_true", help="List tab names and exit.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.spreadsheet:
        logger.error("No spreadsheet given. Set SPREADSHEET_ID in .env or pass --spreadsheet.")
        return 1

    spreadsheet_id = extract_spreadsheet_id(args.spreadsheet)
    service = get_google_service()
    available = get_sheet_names(service, spreadsheet_id)

    if args.list:
        for name in available:
            print(name)
        return 0

    if args.all:
        sheet_names = available
    elif args.sheet:
        sheet_names = resolve_sheets(args.sheet, available)
    else:
        sheet_names = choose_sheets_interactively(available)

    if not sheet_names:
        logger.error("No tab selected.")
        return 1

    records: List[Dict] = []
    for sheet_name in sheet_names:
        values = read_sheet_values(service, spreadsheet_id, sheet_name)
        records.extend(parse_sheet(sheet_name, values))

    if not records:
        logger.warning("No client rows found in: %s", ", ".join(sheet_names))
        return 0

    df = build_dataframe(records)

    logger.info(
        "Total %s clients | %s",
        len(df),
        df["client_status"].value_counts().to_dict(),
    )

    save_output(df, sheet_names)
    return 0


if __name__ == "__main__":
    sys.exit(main())
