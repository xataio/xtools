import argparse
from xata.client import XataClient
from json import loads
from textwrap import wrap
from PyPDF2 import PdfReader
from io import BytesIO

parser = argparse.ArgumentParser()
parser.add_argument("--db", help="Database endpoint.", required=True)
parser.add_argument("--branch", help="Branch name.", required=False, default="main")
parser.add_argument("--table", help="Source table name.", required=True)
parser.add_argument(
    "--columns", help="Columns to retrieve files from (comma separated).", required=True
)
parser.add_argument("--dest", help="Destination table name.", required=False)
parser.add_argument(
    "--encoding",
    help="Encoding to use for reading text files",
    required=False,
    default="ascii",
)
parser.add_argument(
    "--id",
    help="Whether to use deterministic or random record ids.",
    required=False,
    choices=["deterministic", "random"],
    default="deterministic",
)
parser.add_argument(
    "--maxchunk", help="Maximum text chunk size.", required=False, default=200000
)
parser.add_argument(
    "--mode",
    help="Write to Xata using atomic requests or transations.",
    required=False,
    choices=["atomic", "transaction"],
    default="transaction",
)
parser.add_argument(
    "--tsize",
    help="Maximum number of records to emit in a transaction.",
    required=False,
    default=100,
)
parser.add_argument(
    "--psize",
    help="Maximum number of records per page when scrolling.",
    required=False,
    default=200,
)
args = parser.parse_args()

TARGET_DB = str(args.db)

SOURCE_TABLE = str(args.table)

BRANCH = str(args.branch)

if args.dest:
    TARGET_TABLE = str(args.dest)
else:
    TARGET_TABLE = SOURCE_TABLE + "Index"

ID_STRATEGY = str(args.id)

ENCODING = str(args.encoding)

COLUMNS_TO_INDEX = [s.strip() for s in str(args.columns).split(",")]

MAX_TEXT_COLUMN_LENGTH = int(args.maxchunk)

MODE = str(args.mode)

TSIZE = int(args.tsize)
if (TSIZE < 1 or TSIZE > 1000) and MODE == "transaction":
    print(
        "Invalid tsize parameter",
        TSIZE,
        ", the number of operations in a transaction must be between 1 and 1000. Using the default instead (100).",
    )
    TSIZE = 100

PAGE_SIZE = int(args.psize)

SUPPORTED_MEDIA_TYPES = [
    "text/plain",
    "text/csv",
    "application/pdf",
]


def process_file(file, mediaType):
    if mediaType == "text/plain" or mediaType == "text/csv":
        chunked_text = process_text_file(file)
    elif mediaType == "application/pdf":
        chunked_text = process_pdf_file(file)
    else:
        chunked_text = []
    return chunked_text


def process_text_file(file):
    chunked_text = wrap(
        str(file.content.decode(ENCODING)),
        width=MAX_TEXT_COLUMN_LENGTH,
        drop_whitespace=False,
        break_on_hyphens=False,
        expand_tabs=False,
        replace_whitespace=False,
    )
    return chunked_text


def process_pdf_file(file):
    with BytesIO(file.content) as open_pdf_file:
        reader = PdfReader(open_pdf_file)
        chunked_text = []
        for page_iterator in range(len(reader.pages)):
            pdf_page = reader.pages[page_iterator]
            extracted_text = pdf_page.extract_text()
            chunks = wrap(
                str(extracted_text),
                width=MAX_TEXT_COLUMN_LENGTH,
                drop_whitespace=False,
                break_on_hyphens=False,
                expand_tabs=False,
                replace_whitespace=False,
            )
            chunked_text.extend(chunks)
        return chunked_text


def ingest_chunks(
    xata,
    chunks,
    source_record,
    column_type,
    column,
    column_file,
):
    chunk_iterator = 0
    if MODE == "transaction":
        transaction_payload = {}
        transaction_payload["operations"] = []
    for chunk in chunks:
        content_record = {
            "content": chunk,
            "source": source_record["id"],
            "origin_column": column,
            "filename": column_file["name"],
        }
        if ID_STRATEGY == "deterministic":
            if column_type == "single_file":
                chunk_rec_id = (
                    f'{source_record["id"]}-{SOURCE_TABLE}-{column}-{chunk_iterator}'
                )
            elif column_type == "multiple_files":
                chunk_rec_id = f'{source_record["id"]}-{SOURCE_TABLE}-{column}-{column_file["id"]}-{chunk_iterator}'
            if MODE == "atomic":
                resp = xata.records().upsert(
                    TARGET_TABLE, chunk_rec_id, content_record, branch_name=BRANCH
                )
                if resp.status_code in (200, 201):
                    print(
                        "  id:",
                        chunk_rec_id,
                        "size:",
                        len(content_record["content"]),
                        "chars",
                    )
                else:
                    print("Response", resp.status_code, resp)
            elif MODE == "transaction":
                transaction_payload["operations"].append(
                    {
                        "update": {
                            "table": TARGET_TABLE,
                            "id": chunk_rec_id,
                            "fields": content_record,
                            "upsert": True,
                        }
                    }
                )
        elif ID_STRATEGY == "random":
            if MODE == "atomic":
                resp = xata.records().insert(
                    TARGET_TABLE, content_record, branch_name=BRANCH
                )
                if resp.status_code == 201:
                    print(
                        "  id:",
                        resp["id"],
                        "size:",
                        len(content_record["content"]),
                        "chars",
                    )
                else:
                    print("Response", resp.status_code, resp)
            elif MODE == "transaction":
                transaction_payload["operations"].append(
                    {
                        "insert": {
                            "table": TARGET_TABLE,
                            "record": content_record,
                        }
                    }
                )
        if MODE == "transaction" and (
            len(transaction_payload["operations"]) == TSIZE
            or chunk_iterator == (len(chunks) - 1)
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
            transaction_payload = {}
            transaction_payload["operations"] = []
        chunk_iterator += 1


def ensure_target_table(xata: XataClient):
    target_table_schema = {
        "columns": [
            {
                "name": "content",
                "type": "text",
            },
            {"name": "filename", "type": "string"},
            {"name": "origin_column", "type": "string"},
            {"name": "source", "type": "link", "link": {"table": SOURCE_TABLE}},
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


def process_response(xata, response):
    for record in response["records"]:
        for column in COLUMNS_TO_INDEX:
            column_files = []
            if column in record and type(record[column]) == dict:
                column_type = "single_file"
                column_files.append(record[column])
            elif column in record and type(record[column]) == list:
                column_type = "multiple_files"
                for each_file in record[column]:
                    column_files.append(each_file)

            for column_file in column_files:
                if "mediaType" in column_file:
                    mediaType = column_file["mediaType"]
                else:
                    mediaType = "unknown"
                if mediaType in SUPPORTED_MEDIA_TYPES:
                    print(
                        "\nDownloading file from table:",
                        SOURCE_TABLE,
                        ", record:",
                        record["id"],
                        ", column:",
                        column,
                        ", filename:",
                        column_file["name"],
                    )
                    if column_type == "single_file":
                        file = xata.files().get(
                            table_name=SOURCE_TABLE,
                            column_name=column,
                            record_id=record["id"],
                            branch_name=BRANCH,
                        )
                    elif column_type == "multiple_files":
                        file = xata.files().get_item(
                            table_name=SOURCE_TABLE,
                            column_name=column,
                            record_id=record["id"],
                            file_id=column_file["id"],
                            branch_name=BRANCH,
                        )
                    if file.is_success():
                        chunks = process_file(file, mediaType)
                        print(
                            "- Processing record:",
                            record["id"],
                            ", column:",
                            column,
                            ", filename:",
                            column_file["name"],
                            ", type:",
                            mediaType,
                            "in",
                            len(chunks),
                            "chunk:" if len(chunks) == 1 else "chunks:",
                        )
                        ingest_chunks(
                            xata,
                            chunks,
                            record,
                            column_type,
                            column,
                            column_file,
                        )
                    else:
                        print(
                            "...Error",
                            file.response,
                            "while fetching file, skipping it.",
                        )
                else:
                    print(
                        "\nUnsupported media type",
                        column_file["mediaType"],
                        "skipping file",
                        column_file["name"],
                        "from record:",
                        record["id"],
                        "column:",
                        column,
                    )


def main():
    xata = XataClient(db_url=TARGET_DB, branch_name=BRANCH)
    ensure_target_table(xata)
    querypayload = {"columns": COLUMNS_TO_INDEX, "page": {"size": PAGE_SIZE}}
    more = True
    while more:
        response = xata.data().query(SOURCE_TABLE, querypayload, branch_name=BRANCH)
        if "records" in response:
            process_response(xata, response)
        page = {"after": response.get_cursor(), "size": PAGE_SIZE}
        querypayload = {"columns": COLUMNS_TO_INDEX, "page": page}
        more = response.has_more_results()


if __name__ == "__main__":
    main()
