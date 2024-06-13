#! /usr/bin/env python

import os
import argparse
from methods import get, post, put
from threads import producer, consumer, reporter
from strategy import compute_table_link_depth
from time import sleep
from threading import Thread
from queue import Queue
from datetime import datetime
import json
from operator import itemgetter

parser = argparse.ArgumentParser()

# Adding mandatory argument
parser.add_argument("--from_workspace", help="Source workspace id", required=True)
parser.add_argument("--from_database", help="Source database name", required=True)
parser.add_argument("--from_branch", help="Source branch name", required=True)
parser.add_argument("--from_region", help="Source region name", required=True)
parser.add_argument(
    "--from_XATA_API_KEY", help="Xata API key for the source workspace", required=False
)
parser.add_argument("--to_workspace", help="Destination workspace id", required=False)
parser.add_argument("--to_database", help="Destination database name", required=False)
parser.add_argument("--to_branch", help="Destinatione branch name", required=False)
parser.add_argument("--to_region", help="Destination region name", required=False)
parser.add_argument(
    "--to_XATA_API_KEY",
    help="Xata API key for the destination workspace",
    required=False,
)
parser.add_argument(
    "--concurrency",
    help="Number of concurrent writer threads per table. Range is 1 to 10.",
    required=False,
)
parser.add_argument(
    "--bulk_size",
    help="Number of records per write request. Range is 1 to 1000.",
    required=False,
)
parser.add_argument(
    "--page_size",
    help="Number of records to fetch in each page of the scroll request. Range is 1 to 200.",
    required=False,
)
parser.add_argument(
    "--queue_size",
    help="Number of inflight events we can store in the memory queue. Range is page_size to 10000.",
    required=False,
)
parser.add_argument("--error_file", help="File path to output errors.", required=False)
parser.add_argument(
    "--output",
    help="Where to output records: xata or local files. Options: xata,file",
    required=True,
)
parser.add_argument(
    "--output_path", help="Directory to write record content to.", required=False
)
parser.add_argument(
    "--output_format", help="Directory to write record content to.", required=False
)
parser.add_argument(
    "--links_backfill_method",
    help="How to backfill links: bulk updates using transactions, bulk rewrite entire records, atomic link column updates. Options: transaction,bulk,atomic. Default: transaction",
    required=False,
)
parser.add_argument(
    "--custom_source",
    help="Using a custom source instead of Xata's Production endpoints",
    required=False,
)
parser.add_argument(
    "--custom_source_host_header",
    help="Using a custom host header for connections to the source",
    required=False,
)
parser.add_argument(
    "--custom_destination",
    help="Using a custom destination instead of Xata's Production endpoints",
    required=False,
)
parser.add_argument(
    "--custom_destination_host_header",
    help="Using a custom host header for connections to the destination",
    required=False,
)
parser.add_argument(
    "--custom_control_plane",
    help="Using a custom control plane address",
    required=False,
)

args = parser.parse_args()

if str(args.output).lower() == "xata":
    OUTPUT = "xata"
    OUTPUT_PATH = ""
    OUTPUT_FORMAT = "json"
elif str(args.output).lower() == "file":
    OUTPUT = "file"
    if str(args.output_format).lower() == "csv":
        OUTPUT_FORMAT = "csv"
    else:
        OUTPUT_FORMAT = "json"
else:
    print("Please specify a valid --output target value (xata or file).")
    exit(-1)

if not args.from_XATA_API_KEY:
    try:
        from_XATA_API_KEY = str(os.environ["from_XATA_API_KEY"])
    except KeyError:
        print(
            "from_XATA_API_KEY must be passed as argument or set as env variable in order to access the source workspace"
        )
else:
    from_XATA_API_KEY = str(args.from_XATA_API_KEY)

FROM_WORKSPACE = str(args.from_workspace)
FROM_DATABASE = str(args.from_database)
FROM_BRANCH = str(args.from_branch)
FROM_REGION = str(args.from_region)

if OUTPUT == "xata":
    if not args.to_XATA_API_KEY:
        try:
            to_XATA_API_KEY = str(os.environ["to_XATA_API_KEY"])
        except KeyError:
            print(
                "to_XATA_API_KEY must be passed as argument or set as env variable in order to access the destination workspace"
            )
            exit(-1)
    else:
        to_XATA_API_KEY = str(args.to_XATA_API_KEY)
    if not args.to_workspace:
        print("Target workspace id must be provided when output is set to xata.")
        exit(-1)
    else:
        TO_WORKSPACE = str(args.to_workspace)
    if not args.to_database:
        print("Target database must be provided when output is set to xata.")
        exit(-1)
    else:
        TO_DATABASE = str(args.to_database)
    if not args.to_branch:
        print("Target branch must be provided when output is set to xata.")
        exit(-1)
    else:
        TO_BRANCH = str(args.to_branch)
    if not args.to_region:
        print("Target region must be provided when output is set to xata.")
        exit(-1)
    else:
        TO_REGION = str(args.to_region)
elif OUTPUT == "file":
    to_XATA_API_KEY = ""
    TO_WORKSPACE = FROM_WORKSPACE
    TO_DATABASE = FROM_DATABASE
    TO_BRANCH = FROM_BRANCH
    TO_REGION = FROM_REGION
    if not args.output_path:
        OUTPUT_PATH = (
            "output/"
            + FROM_WORKSPACE
            + "-"
            + FROM_DATABASE
            + "-"
            + FROM_REGION
            + "-"
            + FROM_BRANCH
            + "-"
            + str(datetime.now().isoformat(timespec="seconds"))
            + "/"
        )
    else:
        OUTPUT_PATH = str(args.output_path)
        if not OUTPUT_PATH.endswith("/"):
            OUTPUT_PATH += "/"

if not args.concurrency and OUTPUT == "xata":
    CONCURRENT_CONSUMERS = 2
elif OUTPUT == "file":
    CONCURRENT_CONSUMERS = 1
elif int(args.concurrency) >= 1 and int(args.concurrency) <= 10:
    CONCURRENT_CONSUMERS = int(args.concurrency)
else:
    print("Error: Concurrency should be between 1 and 10.")
    exit(-1)

if not args.bulk_size:
    BULK_SIZE = 100
elif int(args.bulk_size) >= 1 and int(args.bulk_size) <= 1000:
    BULK_SIZE = int(args.bulk_size)
    if BULK_SIZE > 500:
        print(
            "Warning: You have configured a large bulk size. If there are large records in the tables to copy, you could reach the transaction size limit. In case errors occur, consider retrying with a smaller bulk size."
        )
else:
    print("Error: Bulk size should be between 1 and 1000.")
    exit(-1)

if not args.page_size:
    PAGE_SIZE = 200
elif int(args.page_size) >= 1 and int(args.page_size) <= 200:
    PAGE_SIZE = int(args.page_size)
else:
    print("Error: Page size should be between 1 and 200.")
    exit(-1)

if not args.queue_size:
    MAX_QUEUE_SIZE = 1000
elif int(args.queue_size) >= PAGE_SIZE and int(args.queue_size) <= 10000:
    MAX_QUEUE_SIZE = int(args.queue_size)
else:
    print(
        "Error: Queue size should be between the page size of",
        PAGE_SIZE,
        "and the limit of 10000.",
    )
    exit(-1)

if not args.error_file:
    ERROR_FILE = (
        "logs/debug-"
        + TO_WORKSPACE
        + "-"
        + TO_DATABASE
        + "-"
        + TO_BRANCH
        + "-"
        + str(datetime.now().isoformat(timespec="seconds"))
        + ".log"
    )
else:
    ERROR_FILE = str(args.error_file)

try:
    os.makedirs(os.path.dirname(ERROR_FILE), exist_ok=True)
    f = open(ERROR_FILE, "w+")
except:
    print("Cannot open or create error log file", ERROR_FILE)
    exit(-1)

if not args.links_backfill_method:
    BACKFILL = "bulk_transaction"
elif str(args.links_backfill_method).lower() == "bulk":
    BACKFILL = "bulk_rewrite"
elif str(args.links_backfill_method).lower() == "atomic":
    BACKFILL = "atomic_update"
elif str(args.links_backfill_method).lower() == "transaction":
    BACKFILL = "bulk_transaction"
else:
    print("Error: links_backfill_method should be one of bulk or atomic.")
    exit(-1)

if OUTPUT == "file":
    try:
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    except:
        print("Cannot access or create output directory at", OUTPUT_PATH)
        exit(-1)

DEFAULT_BASE_URL_DOMAIN = "xata.sh"
DEFAULT_CONTROL_PLANE_DOMAIN = "api.xata.io"

if args.custom_source:
    from_BRANCH_URL = (
        str(args.custom_source) + "/db/" + FROM_DATABASE + ":" + FROM_BRANCH
    )
    if args.custom_source_host_header:
        source_host_header = str(args.custom_source_host_header)
    else:
        source_host_header = (
            FROM_WORKSPACE + "." + FROM_REGION + "." + DEFAULT_BASE_URL_DOMAIN
        )
else:
    from_BRANCH_URL = (
        "https://"
        + FROM_WORKSPACE
        + "."
        + FROM_REGION
        + "."
        + DEFAULT_BASE_URL_DOMAIN
        + "/db/"
        + FROM_DATABASE
        + ":"
        + FROM_BRANCH
    )
    source_host_header = ""

if args.custom_destination:
    to_BRANCH_URL = (
        str(args.custom_destination) + "/db/" + TO_DATABASE + ":" + TO_BRANCH
    )
    if args.custom_destination_host_header:
        destination_host_header = str(args.custom_destination_host_header)
    else:
        destination_host_header = (
            TO_WORKSPACE + "." + TO_REGION + "." + DEFAULT_BASE_URL_DOMAIN
        )
else:
    to_BRANCH_URL = (
        "https://"
        + TO_WORKSPACE
        + "."
        + TO_REGION
        + "."
        + DEFAULT_BASE_URL_DOMAIN
        + "/db/"
        + TO_DATABASE
        + ":"
        + TO_BRANCH
    )
    destination_host_header = ""

if args.custom_control_plane:
    to_DB_MANAGEMENT_URL = (
        str(args.custom_control_plane)
        + "/workspaces/"
        + TO_WORKSPACE
        + "/dbs/"
        + TO_DATABASE
    )
else:
    to_DB_MANAGEMENT_URL = (
        "https://"
        + DEFAULT_CONTROL_PLANE_DOMAIN
        + "/workspaces/"
        + TO_WORKSPACE
        + "/dbs/"
        + TO_DATABASE
    )

# verify the origin branch exists
branch_check_response, errors = get(
    apikey=from_XATA_API_KEY,
    urlPath=from_BRANCH_URL,
    expect_codes=[],
    ERROR_FILE=ERROR_FILE,
)
if branch_check_response.status_code != 200:
    print(
        "Aborting because the origin branch "
        + str(from_BRANCH_URL)
        + " was not fround: ",
        branch_check_response,
    )
    exit(-1)

target_type = "unidentified"

##################
if OUTPUT == "xata":
    # If we are writting to a different database, either in the same workspace or in another one, we require that the target db does not exist
    if FROM_WORKSPACE != TO_WORKSPACE or FROM_DATABASE != TO_DATABASE:
        db_create_response, errors = put(
            apikey=to_XATA_API_KEY,
            urlPath=to_DB_MANAGEMENT_URL,
            expect_codes=[422],
            payload={"region": TO_REGION, "branchName": TO_BRANCH},
            ERROR_FILE=ERROR_FILE,
        )
        if db_create_response.status_code == 422:
            print(
                "Aborting because the target database",
                TO_DATABASE,
                "already exists in workspace",
                TO_WORKSPACE,
            )
            exit(-1)
        target_type = "new_database"
    # If we are writting to the same database we require the target branch does not exist
    elif (
        FROM_WORKSPACE == TO_WORKSPACE
        and FROM_DATABASE == TO_DATABASE
        and FROM_BRANCH != TO_BRANCH
    ):
        if FROM_REGION != TO_REGION:
            print(
                "Origin and target region must match when origin and target database is the same."
            )
            exit(-1)
        branch_check_response, errors = get(
            apikey=to_XATA_API_KEY,
            urlPath=to_BRANCH_URL,
            expect_codes=[],
            ERROR_FILE=ERROR_FILE,
            host_header=destination_host_header,
        )
        if branch_check_response.status_code != 404:
            print(
                "Aborting because we couldn't verify the target branch "
                + str(to_BRANCH_URL)
                + " does not exist: ",
                branch_check_response,
            )
            exit(-1)
        target_type = "same_database_new_branch"
    # Origin and target branch should not match
    else:
        print(
            "Aborting, origin and target branch cannot be the same.",
            " Origin: ",
            FROM_WORKSPACE,
            FROM_DATABASE,
            FROM_BRANCH,
            " Target: ",
            TO_WORKSPACE,
            TO_DATABASE,
            TO_BRANCH,
        )
        exit(-1)
#################

print("\n>>> Xata Replay tool <<<\n")

# List parameters
print("Using configuration:")
print(
    " Concurrent writers per table:",
    CONCURRENT_CONSUMERS,
    "\n Bulk size:",
    BULK_SIZE,
    "\n Page scroll size:",
    PAGE_SIZE,
    "\n Inflight records queue size:",
    MAX_QUEUE_SIZE,
    "\n Error logs written to:",
    ERROR_FILE,
)

# Copy schema
print("\nApplying schema:")
print("- Retrieving schema from origin", from_BRANCH_URL)
from_schema_raw, errors = get(
    apikey=from_XATA_API_KEY,
    urlPath=from_BRANCH_URL,
    ERROR_FILE=ERROR_FILE,
    host_header=source_host_header,
)
from_schema = from_schema_raw.json()
tables_migration = {}
tables_migration["operations"] = []
columns_migration = {}
columns_migration["operations"] = []
tables = []
schema_links = {}
schema_files = {}
if len(from_schema["schema"]["tables"]) == 0:
    print(
        "Aborting, the origin database branch: ",
        from_BRANCH_URL,
        " does not contain any table",
    )
    exit(-1)

# Create tables and columns in two sequential migration requests
for table in from_schema["schema"]["tables"]:
    schema_links[table["name"]] = {}
    schema_files[table["name"]] = {}
    tables.append(table["name"])
    tables_migration["operations"].append({"addTable": {"table": table["name"]}})
    for column in table["columns"]:
        columns_migration["operations"].append(
            {"addColumn": {"table": table["name"], "column": column}}
        )
        # Column types that require special handling in record response: link
        if column["type"] == "link":
            schema_links[table["name"]][column["name"]] = column
        # Mark file columns for exclusion
        if column["type"] == "file" or column["type"] == "file[]":
            schema_files[table["name"]][column["name"]] = column

if OUTPUT == "xata":
    print("- Creating schema in target", to_BRANCH_URL)
    # Create a new database and initialize tables and table schema
    if target_type == "new_database":
        #############################
        tables_migration_response, errors = post(
            apikey=to_XATA_API_KEY,
            urlPath=to_BRANCH_URL + "/schema/update",
            payload=tables_migration,
            ERROR_FILE=ERROR_FILE,
            host_header=destination_host_header,
        )
        if tables_migration_response.json()["status"] != "completed":
            print(
                "Error when creating tables. Unexpected status in /schema/update API response."
            )
            exit(-1)
        columns_migration_response, errors = post(
            apikey=to_XATA_API_KEY,
            urlPath=to_BRANCH_URL + "/schema/update",
            payload=columns_migration,
            ERROR_FILE=ERROR_FILE,
            host_header=destination_host_header,
        )
        if columns_migration_response.json()["status"] != "completed":
            print(
                "Error when applying schema to tables. Unexpected status in /schema/update API response."
            )
            exit(-1)
        #############################
    # Create database branch from existing branch
    elif target_type == "same_database_new_branch":
        from_branch_payload = {"from": FROM_BRANCH}
        branch_create_response, errors = put(
            apikey=to_XATA_API_KEY,
            urlPath=to_BRANCH_URL,
            payload=from_branch_payload,
            expect_codes=[],
            ERROR_FILE=ERROR_FILE,
            host_header=destination_host_header,
        )
        if (
            branch_create_response.status_code != 201
            or branch_create_response.json()["status"] != "completed"
        ):
            print(
                "Error while creating target branch "
                + to_BRANCH_URL
                + " from origin "
                + FROM_BRANCH
                + " :",
                branch_create_response,
            )
            exit(-1)
    else:
        print(
            "Aborting: Could not identify the target branch. It does not appear to be a new branch in a new or an existing database."
        )
        exit(-1)

    to_schema_raw, errors = get(
        apikey=to_XATA_API_KEY,
        urlPath=to_BRANCH_URL,
        ERROR_FILE=ERROR_FILE,
        host_header=destination_host_header,
    )
    to_schema = to_schema_raw.json()
    if from_schema["schema"] == to_schema["schema"]:
        print("- Schema has been copied successfully")
elif OUTPUT == "file":
    print("- Writing schema to", OUTPUT_PATH + "schema.json")
    with open(OUTPUT_PATH + "schema.json", "w") as f:
        f.write(json.dumps(from_schema["schema"]))
        to_schema = from_schema
    if OUTPUT_FORMAT == "csv":
        for table in from_schema["schema"]["tables"]:
            with open(OUTPUT_PATH + table["name"] + ".csv", "a") as f:
                column_counter = 0
                f.write("id,")
                for column in sorted(table["columns"], key=itemgetter("name")):
                    column_counter += 1
                    if column_counter < len(table["columns"]):
                        f.write(str(column["name"]) + ",")
                    else:
                        f.write(str(column["name"]) + "\n")
# Category 1: tables without links
# Category 2: tables with links to tables without links
# Category 3: tables with links to table with links. Category 3 tables require a second pass, meaning we index them without links and after all tables have been indexed, we create links.
table_categories = compute_table_link_depth(to_schema)

# We ingest tables from category 1 and 2 in this order, so that links of tables in category 2 can be created.
# Category 3 is not handled yet
category_order = ["category1", "category2", "category3"]
table_queues = {}
table_threads = {}
reporting_queue = Queue(len(tables))
reporter = Thread(
    target=reporter,
    args=(
        reporting_queue,
        tables,
        table_categories,
        from_XATA_API_KEY,
        from_BRANCH_URL,
        ERROR_FILE,
        CONCURRENT_CONSUMERS,
        source_host_header,
        OUTPUT,
        OUTPUT_FORMAT,
        OUTPUT_PATH,
    ),
)
reporter.start()
for category in category_order:
    # Logic for replaying data to Xata
    if OUTPUT == "xata":
        # Expect all threads of the current table category to complete before starting the next category
        if category == "category1" or category == "category2":
            for table in table_categories[category]:
                table_queues[table] = Queue(MAX_QUEUE_SIZE)
                table_threads[table] = {}
                table_threads[table]["consumers"] = {}
                for consumer_iterator in range(CONCURRENT_CONSUMERS):
                    table_threads[table]["consumers"][str(consumer_iterator)] = Thread(
                        target=consumer,
                        args=(
                            table_queues[table],
                            reporting_queue,
                            BULK_SIZE,
                            to_XATA_API_KEY,
                            to_BRANCH_URL,
                            table,
                            to_schema,
                            schema_links,
                            schema_files,
                            table_categories,
                            OUTPUT,
                            OUTPUT_FORMAT,
                            OUTPUT_PATH,
                            ERROR_FILE,
                            destination_host_header,
                        ),
                    )
                    table_threads[table]["consumers"][str(consumer_iterator)].start()
                table_threads[table]["producer"] = Thread(
                    target=producer,
                    args=(
                        table_queues[table],
                        PAGE_SIZE,
                        from_XATA_API_KEY,
                        from_BRANCH_URL,
                        table,
                        schema_links,
                        schema_files,
                        ERROR_FILE,
                        source_host_header,
                    ),
                )
                table_threads[table]["producer"].start()
            for table in table_categories[category]:
                for consumer_iterator in range(CONCURRENT_CONSUMERS):
                    table_threads[table]["consumers"][str(consumer_iterator)].join()
                table_threads[table]["producer"].join()
        elif category == "category3":
            for table in table_categories[category]:
                # First pass: write category 3 table content without creating links
                table_queues[table] = Queue(MAX_QUEUE_SIZE)
                table_threads[table] = {}
                table_threads[table]["consumers"] = {}
                for consumer_iterator in range(CONCURRENT_CONSUMERS):
                    table_threads[table]["consumers"][str(consumer_iterator)] = Thread(
                        target=consumer,
                        args=(
                            table_queues[table],
                            reporting_queue,
                            BULK_SIZE,
                            to_XATA_API_KEY,
                            to_BRANCH_URL,
                            table,
                            to_schema,
                            schema_links,
                            schema_files,
                            table_categories,
                            OUTPUT,
                            OUTPUT_FORMAT,
                            OUTPUT_PATH,
                            ERROR_FILE,
                            destination_host_header,
                            "no_links",
                        ),
                    )
                    table_threads[table]["consumers"][str(consumer_iterator)].start()
                table_threads[table]["producer"] = Thread(
                    target=producer,
                    args=(
                        table_queues[table],
                        PAGE_SIZE,
                        from_XATA_API_KEY,
                        from_BRANCH_URL,
                        table,
                        schema_links,
                        schema_files,
                        ERROR_FILE,
                        source_host_header,
                    ),
                )
                table_threads[table]["producer"].start()
            for table in table_categories[category]:
                for consumer_iterator in range(CONCURRENT_CONSUMERS):
                    table_threads[table]["consumers"][str(consumer_iterator)].join()
                table_threads[table]["producer"].join()
            if BACKFILL == "atomic_update":
                # Second pass: update links in category 3 tables with an atomic request per record.
                for table in table_categories[category]:
                    table_queues[table] = Queue(MAX_QUEUE_SIZE)
                    table_threads[table] = {}
                    table_threads[table]["consumers"] = {}
                    for consumer_iterator in range(CONCURRENT_CONSUMERS):
                        table_threads[table]["consumers"][str(consumer_iterator)] = (
                            Thread(
                                target=consumer,
                                args=(
                                    table_queues[table],
                                    reporting_queue,
                                    BULK_SIZE,
                                    to_XATA_API_KEY,
                                    to_BRANCH_URL,
                                    table,
                                    to_schema,
                                    schema_links,
                                    schema_files,
                                    table_categories,
                                    OUTPUT,
                                    OUTPUT_FORMAT,
                                    OUTPUT_PATH,
                                    ERROR_FILE,
                                    destination_host_header,
                                    "only_links",
                                ),
                            )
                        )
                        table_threads[table]["consumers"][
                            str(consumer_iterator)
                        ].start()
                    table_threads[table]["producer"] = Thread(
                        target=producer,
                        args=(
                            table_queues[table],
                            PAGE_SIZE,
                            from_XATA_API_KEY,
                            from_BRANCH_URL,
                            table,
                            schema_links,
                            schema_files,
                            ERROR_FILE,
                            source_host_header,
                            "only_links",
                        ),
                    )
                    table_threads[table]["producer"].start()
                for table in table_categories[category]:
                    for consumer_iterator in range(CONCURRENT_CONSUMERS):
                        table_threads[table]["consumers"][str(consumer_iterator)].join()
                    table_threads[table]["producer"].join()
            elif BACKFILL == "bulk_transaction":
                # Second pass: update links in category 3 tables with bulk updates using transactions
                for table in table_categories[category]:
                    table_queues[table] = Queue(MAX_QUEUE_SIZE)
                    table_threads[table] = {}
                    table_threads[table]["consumers"] = {}
                    for consumer_iterator in range(CONCURRENT_CONSUMERS):
                        table_threads[table]["consumers"][str(consumer_iterator)] = (
                            Thread(
                                target=consumer,
                                args=(
                                    table_queues[table],
                                    reporting_queue,
                                    BULK_SIZE,
                                    to_XATA_API_KEY,
                                    to_BRANCH_URL,
                                    table,
                                    to_schema,
                                    schema_links,
                                    schema_files,
                                    table_categories,
                                    OUTPUT,
                                    OUTPUT_FORMAT,
                                    OUTPUT_PATH,
                                    ERROR_FILE,
                                    destination_host_header,
                                    "bulk_links_transaction",
                                ),
                            )
                        )
                        table_threads[table]["consumers"][
                            str(consumer_iterator)
                        ].start()
                    table_threads[table]["producer"] = Thread(
                        target=producer,
                        args=(
                            table_queues[table],
                            PAGE_SIZE,
                            from_XATA_API_KEY,
                            from_BRANCH_URL,
                            table,
                            schema_links,
                            schema_files,
                            ERROR_FILE,
                            source_host_header,
                            "only_links",
                        ),
                    )
                    table_threads[table]["producer"].start()
                for table in table_categories[category]:
                    for consumer_iterator in range(CONCURRENT_CONSUMERS):
                        table_threads[table]["consumers"][str(consumer_iterator)].join()
                    table_threads[table]["producer"].join()
            elif BACKFILL == "bulk_rewrite":
                # Second pass: update links in category 3 tables by rewritting the entire record in bulks including links.
                for table in table_categories[category]:
                    table_queues[table] = Queue(MAX_QUEUE_SIZE)
                    table_threads[table] = {}
                    table_threads[table]["consumers"] = {}
                    for consumer_iterator in range(CONCURRENT_CONSUMERS):
                        table_threads[table]["consumers"][str(consumer_iterator)] = (
                            Thread(
                                target=consumer,
                                args=(
                                    table_queues[table],
                                    reporting_queue,
                                    BULK_SIZE,
                                    to_XATA_API_KEY,
                                    to_BRANCH_URL,
                                    table,
                                    to_schema,
                                    schema_links,
                                    schema_files,
                                    table_categories,
                                    OUTPUT,
                                    OUTPUT_FORMAT,
                                    OUTPUT_PATH,
                                    ERROR_FILE,
                                    destination_host_header,
                                    "full",
                                    "with_links",
                                ),
                            )
                        )
                        table_threads[table]["consumers"][
                            str(consumer_iterator)
                        ].start()
                    table_threads[table]["producer"] = Thread(
                        target=producer,
                        args=(
                            table_queues[table],
                            PAGE_SIZE,
                            from_XATA_API_KEY,
                            from_BRANCH_URL,
                            table,
                            schema_links,
                            schema_files,
                            ERROR_FILE,
                            source_host_header,
                            "full",
                            "with_links",
                        ),
                    )
                    table_threads[table]["producer"].start()
                for table in table_categories[category]:
                    for consumer_iterator in range(CONCURRENT_CONSUMERS):
                        table_threads[table]["consumers"][str(consumer_iterator)].join()
                    table_threads[table]["producer"].join()
    # Logic for writting to files on disk
    elif OUTPUT == "file":
        for table in table_categories[category]:
            table_queues[table] = Queue(MAX_QUEUE_SIZE)
            table_threads[table] = {}
            table_threads[table]["consumers"] = {}
            for consumer_iterator in range(CONCURRENT_CONSUMERS):
                table_threads[table]["consumers"][str(consumer_iterator)] = Thread(
                    target=consumer,
                    args=(
                        table_queues[table],
                        reporting_queue,
                        BULK_SIZE,
                        to_XATA_API_KEY,
                        to_BRANCH_URL,
                        table,
                        to_schema,
                        schema_links,
                        schema_files,
                        table_categories,
                        OUTPUT,
                        OUTPUT_FORMAT,
                        OUTPUT_PATH,
                        ERROR_FILE,
                        destination_host_header,
                    ),
                )
                table_threads[table]["consumers"][str(consumer_iterator)].start()
            table_threads[table]["producer"] = Thread(
                target=producer,
                args=(
                    table_queues[table],
                    PAGE_SIZE,
                    from_XATA_API_KEY,
                    from_BRANCH_URL,
                    table,
                    schema_links,
                    schema_files,
                    ERROR_FILE,
                    source_host_header,
                ),
            )
            table_threads[table]["producer"].start()
        for table in table_categories[category]:
            for consumer_iterator in range(CONCURRENT_CONSUMERS):
                table_threads[table]["consumers"][str(consumer_iterator)].join()
            table_threads[table]["producer"].join()
reporter.join()
