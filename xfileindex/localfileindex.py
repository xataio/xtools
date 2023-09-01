import argparse
from xata.client import XataClient
from json import loads
from textwrap import wrap
from PyPDF2 import PdfReader

parser = argparse.ArgumentParser()
parser.add_argument("--db", help="Database endpoint.", required=True)
parser.add_argument("--file", help="File to index.", required=True)
parser.add_argument("--branch", help="Branch name.", required=False, default="main")
parser.add_argument("--dest", help="Destination table name.", required=False)
parser.add_argument(
    "--encoding",
    help="Encoding to use for reading text files",
    required=False,
    default="UTF-8",
)
parser.add_argument(
    "--maxchunk", help="Maximum text chunk size.", required=False, default=200000
)
parser.add_argument(
    "--tsize",
    help="Maximum number of records to emit in a transaction.",
    required=False,
    default=100,
)
args = parser.parse_args()

TARGET_DB = str(args.db)

BRANCH = str(args.branch)

FILENAME = str(args.file)

ENCODING = str(args.encoding)

if args.dest:
    TARGET_TABLE = str(args.dest)
else:
    TARGET_TABLE = "Index"

MAX_TEXT_COLUMN_LENGTH = int(args.maxchunk)

TSIZE = int(args.tsize)
if TSIZE < 1 or TSIZE > 1000:
    print(
        "Invalid tsize parameter",
        TSIZE,
        ", the number of operations in a transaction must be between 1 and 1000. Using the default instead (100).",
    )
    TSIZE = 100


def ensure_target_table(xata: XataClient):
    target_table_schema = {
        "columns": [
            {
                "name": "content",
                "type": "text",
            },
            {"name": "filename", "type": "string"},
        ]
    }
    create_table_response = xata.table().create(TARGET_TABLE, branch_name=BRANCH)
    if create_table_response.status_code == 201:
        set_table_schema_resp = xata.table().set_schema(
            TARGET_TABLE, target_table_schema, branch_name=BRANCH
        )
        if not set_table_schema_resp.is_success():
            print(
                "Error: Failed to create target table",
                TARGET_TABLE,
                "with schema",
                target_table_schema,
            )
            exit(-1)
        print("Created new table", TARGET_TABLE)
    elif create_table_response.status_code == 204:
        get_table_schema_response = xata.table().get_schema(
            TARGET_TABLE, branch_name=BRANCH
        )
        if get_table_schema_response.is_success():
            current_schema = loads(get_table_schema_response.content)
            if current_schema != target_table_schema:
                print(
                    "Error: Target table",
                    TARGET_TABLE,
                    "exists but deviates from expected schema.\n",
                    "Existing schema:",
                    current_schema,
                    "\n",
                    "Expected schema:",
                    target_table_schema,
                    "\nAborting.",
                )
                exit(-1)
            else:
                print("Using existing table", TARGET_TABLE)
    else:
        print(
            "Unexpected code",
            create_table_response.status_code,
            "when creating or checking for table",
            TARGET_TABLE,
        )
        exit(-1)


def main():
    xata = XataClient(db_url=TARGET_DB, branch_name=BRANCH)
    ensure_target_table(xata)
    with open(FILENAME, "rb") as file:
        print("Processing file",FILENAME)
        if FILENAME.endswith(".txt") or FILENAME.endswith(".csv"):
            chunks = wrap(
                str(file.read().decode(ENCODING)),
                width=MAX_TEXT_COLUMN_LENGTH,
                drop_whitespace=False,
                break_on_hyphens=False,
                expand_tabs=False,
                replace_whitespace=False,
            )
        elif FILENAME.endswith(".pdf"):
            reader = PdfReader(file)
            chunks = []
            for page_iterator in range(len(reader.pages)):
                pdf_page = reader.pages[page_iterator]
                extracted_text = pdf_page.extract_text()
                chunked_text = wrap(
                    str(extracted_text),
                    width=MAX_TEXT_COLUMN_LENGTH,
                    drop_whitespace=False,
                    break_on_hyphens=False,
                    expand_tabs=False,
                    replace_whitespace=False,
                )
                chunks.extend(chunked_text)
    transaction_payload = {}
    transaction_payload["operations"] = []
    chunk_iterator = 0
    for chunk in chunks:
        content = {
            "content": chunk,
            "filename": FILENAME.rsplit("/", 1)[-1],
        }
        transaction_payload["operations"].append(
            {
                "insert": {
                    "table": TARGET_TABLE,
                    "record": content,
                }
            }
        )
        if len(transaction_payload["operations"]) == TSIZE or chunk_iterator == (
            len(chunks) - 1
        ):
            resp = xata.records().transaction(
                payload=transaction_payload, branch_name=BRANCH
            )
            if resp.status_code == 200:
                print(
                    "  Indexed",
                    chunk_iterator + 1,
                    "/",
                    len(chunks),
                    "chunk."
                    if len(transaction_payload["operations"]) == 1
                    else "chunks.",
                )
            else:
                print("Response", resp.status_code, resp)
                while resp.status_code == 429:
                    print("Throttled. Retrying...")
                    resp = xata.records().transaction(
                        payload=transaction_payload, branch_name=BRANCH
                    ) 
            transaction_payload["operations"] = []
        chunk_iterator += 1


if __name__ == "__main__":
    main()
