from time import sleep
from queue import Empty
from methods import get, post, patch
from datetime import datetime
from operator import itemgetter


def producer(
    queue,
    PAGE_SIZE,
    from_XATA_API_KEY,
    from_BRANCH_URL,
    table,
    schema_links,
    schema_files,
    ERROR_FILE,
    host_header="",
    mode="full",
    fetch_records="all",
):
    query_payload = {}
    query_payload["page"] = {}
    query_payload["page"]["size"] = PAGE_SIZE

    if mode == "only_links":
        query_payload["filter"] = {}
        query_payload["filter"]["$any"] = []
        query_payload["columns"] = []
        for column in schema_links[table]:
            if "filter" in query_payload and "columns" in query_payload:
                query_payload["filter"]["$any"].append({"$exists": column})
                # only fetch the column.id for the linked record to reduce payload size down to necessary
                query_payload["columns"].append(column + ".id")

    if fetch_records == "with_links":
        query_payload["filter"] = {}
        query_payload["filter"]["$any"] = []
        query_payload["columns"] = []

    more = True
    while more == True:
        if mode == "full" and fetch_records == "all":
            raw_query_response, errors = post(
                apikey=from_XATA_API_KEY,
                urlPath=from_BRANCH_URL + "/tables/" + table + "/query",
                payload=query_payload,
                ERROR_FILE=ERROR_FILE,
                host_header=host_header,
            )
            query_response = raw_query_response.json()
            for record in query_response["records"]:
                record.pop("xata", None)
                # flatten link id keys to make them compatible with the write api
                for column in schema_links[table]:
                    if column in record:
                        record[column] = (record.pop(column))["id"]
                # remove file and file[] columns as unsupported
                for column in schema_files[table]:
                    if column in record:
                        record.pop(column)
                queue.put(record)
            if query_response["meta"]["page"]["more"] == True:
                query_payload["page"]["after"] = query_response["meta"]["page"][
                    "cursor"
                ]
            else:
                more = False
        # only fetch records that have links
        elif mode == "full" and fetch_records == "with_links":
            for column in schema_links[table]:
                # retrieve only records that have link column data
                if "filter" in query_payload and "columns" in query_payload:
                    query_payload["filter"]["$any"].append({"$exists": column})
            raw_query_response, errors = post(
                apikey=from_XATA_API_KEY,
                urlPath=from_BRANCH_URL + "/tables/" + table + "/query",
                payload=query_payload,
                ERROR_FILE=ERROR_FILE,
                host_header=host_header,
            )
            query_response = raw_query_response.json()
            for record in query_response["records"]:
                record.pop("xata", None)
                # flatten link id keys to make them compatible with the write api
                for column in schema_links[table]:
                    if column in record:
                        record[column] = (record.pop(column))["id"]
                # remove file and file[] columns as unsupported
                for column in schema_files[table]:
                    if column in record:
                        record.pop(column)
                queue.put(record)
            if query_response["meta"]["page"]["more"] == True:
                query_payload = {}
                query_payload["page"] = {}
                query_payload["page"]["after"] = query_response["meta"]["page"][
                    "cursor"
                ]
            else:
                more = False
        elif mode == "only_links":
            raw_query_response, errors = post(
                apikey=from_XATA_API_KEY,
                urlPath=from_BRANCH_URL + "/tables/" + table + "/query",
                payload=query_payload,
                ERROR_FILE=ERROR_FILE,
                host_header=host_header,
            )
            query_response = raw_query_response.json()
            for record in query_response["records"]:
                record.pop("xata", None)
                for column in schema_links[table]:
                    if column in record:
                        record[column] = (record.pop(column))["id"]
                # remove file and file[] columns as unsupported
                for column in schema_files[table]:
                    if column in record:
                        record.pop(column)
                queue.put(record)
            if query_response["meta"]["page"]["more"] == True:
                query_payload["page"]["after"] = query_response["meta"]["page"][
                    "cursor"
                ]
                # filter is only applied for the initial query, pagination does not need the filter
                if "filter" in query_payload:
                    query_payload.pop("filter")
            else:
                more = False
    queue.put(None)


def consumer(
    queue,
    reporting_queue,
    BULK_SIZE,
    to_XATA_API_KEY,
    to_BRANCH_URL,
    table,
    schema,
    schema_links,
    schema_files,
    table_categories,
    output,
    output_format,
    output_path,
    ERROR_FILE,
    host_header="",
    mode="full",
    records_type="all",
):
    # consume work
    records = []
    while True:
        # get a unit of work
        try:
            record = queue.get(block=False)
        except Empty:
            sleep(0.05)
            continue
        # check for stop
        if record is None:
            # place the None back for other consumer threads to terminate as well
            queue.put(None)
            break
        if mode == "no_links":
            for column in schema_links[table]:
                if (
                    schema_links[table][column]["link"]["table"]
                    in table_categories["category3"]
                ):
                    # ignore the link if it has a value and points to a category 3 table
                    if column in record.keys():
                        record.pop(column)
        # Process full records with or without link content. These are reported as record counts for the reporting thread.
        # This is the only case that we want to log to file, if the file output method is selected.
        if (mode == "full" and records_type == "all") or mode == "no_links":
            records.append(record)
            if len(records) == BULK_SIZE:
                if output == "xata":
                    resp, errors = post(
                        apikey=to_XATA_API_KEY,
                        urlPath=to_BRANCH_URL + "/tables/" + table + "/bulk",
                        payload={"records": records},
                        ERROR_FILE=ERROR_FILE,
                        host_header=host_header,
                    )
                    # If there are errors returned by the request, add them to the reporting queue
                    if errors != {}:
                        error_report = {}
                        error_report = {table: {"errors": errors}}
                        reporting_queue.put(error_report)
                elif output == "file":
                    if output_format == "json":
                        with open(output_path + table + ".log", "a") as f:
                            for record in records:
                                f.write(str(record) + "\n")
                            errors = {}
                    elif output_format == "csv":
                        with open(output_path + table + ".csv", "a") as f:
                            for record in records:
                                csv_record = ""
                                csv_record_position = 1
                                for current_table in schema["schema"]["tables"]:
                                    if current_table["name"] == table:
                                        csv_record_max_position = len(
                                            current_table["columns"]
                                        )
                                        if "id" in record:
                                            csv_record += str(record["id"]) + ","
                                        for schema_column in sorted(
                                            current_table["columns"],
                                            key=itemgetter("name"),
                                        ):
                                            if schema_column["name"] in record:
                                                if (
                                                    schema_column["type"]
                                                    in (
                                                        "multiple",
                                                        "string",
                                                        "text",
                                                        "object",
                                                    )
                                                    and len(
                                                        record[schema_column["name"]]
                                                    )
                                                    > 0
                                                ):
                                                    csv_record += (
                                                        '"'
                                                        + str(
                                                            record[
                                                                schema_column["name"]
                                                            ]
                                                        ).replace('"', '""')
                                                        + '"'
                                                    )
                                                else:
                                                    csv_record += str(
                                                        record[schema_column["name"]]
                                                    )
                                            if (
                                                csv_record_position
                                                < csv_record_max_position
                                            ):
                                                csv_record += ","
                                            csv_record_position += 1
                                f.write(str(csv_record) + "\n")
                            errors = {}
                status_report = {}
                status_report = {table: {"records": len(records)}}
                reporting_queue.put(status_report)
                records = []
        # explicitly process records with link content. This happens in the second pass for backfilling, these are reported as link counts for the reporting thread.
        elif mode == "full" and records_type == "with_links":
            records.append(record)
            if len(records) == BULK_SIZE:
                if output == "xata":
                    resp, errors = post(
                        apikey=to_XATA_API_KEY,
                        urlPath=to_BRANCH_URL + "/tables/" + table + "/bulk",
                        payload={"records": records},
                        ERROR_FILE=ERROR_FILE,
                        host_header=host_header,
                    )
                    # If there are errors returned by the request, add them to the reporting queue
                    if errors != {}:
                        error_report = {}
                        error_report = {table: {"errors": errors}}
                        reporting_queue.put(error_report)
                status_report = {}
                status_report = {table: {"links": len(records)}}
                reporting_queue.put(status_report)
                records = []
        elif mode == "bulk_links_transaction":
            records.append(record)
            if len(records) == BULK_SIZE:
                if output == "xata":
                    # transaction request
                    transaction_payload = {}
                    transaction_payload["operations"] = []
                    for record_to_update in records:
                        transaction_item = {}
                        transaction_item["update"] = {}
                        transaction_item["update"]["table"] = table
                        transaction_item["update"]["id"] = record_to_update.pop("id")
                        transaction_item["update"]["fields"] = record_to_update
                        transaction_item["update"]["upsert"] = True
                        transaction_payload["operations"].append(transaction_item)
                    resp, errors = post(
                        apikey=to_XATA_API_KEY,
                        urlPath=to_BRANCH_URL + "/transaction",
                        payload=transaction_payload,
                        ERROR_FILE=ERROR_FILE,
                        host_header=host_header,
                    )
                    # If there are errors returned by the request, add them to the reporting queue
                    if errors != {}:
                        error_report = {}
                        error_report = {table: {"errors": errors}}
                        reporting_queue.put(error_report)
                status_report = {}
                status_report = {table: {"links": len(records)}}
                reporting_queue.put(status_report)
                records = []
        # Atomic record link updates
        elif mode == "only_links":
            record_id = record.pop("id")
            resp, errors = patch(
                apikey=to_XATA_API_KEY,
                urlPath=to_BRANCH_URL + "/tables/" + table + "/data/" + record_id,
                payload=record,
                ERROR_FILE=ERROR_FILE,
                host_header=host_header,
            )
            if errors != {}:
                error_report = {}
                error_report = {table: {"errors": errors}}
                reporting_queue.put(error_report)
            status_report = {}
            status_report = {table: {"links": 1}}
            reporting_queue.put(status_report)
    # all done
    if (mode == "full" and records_type == "all") or mode == "no_links":
        if output == "xata" and len(records) > 0:
            resp, errors = post(
                apikey=to_XATA_API_KEY,
                urlPath=to_BRANCH_URL + "/tables/" + table + "/bulk",
                payload={"records": records},
                ERROR_FILE=ERROR_FILE,
            )
            # If there are errors returned by the request, add them to the reporting queue
            if errors != {}:
                error_report = {}
                error_report = {table: {"errors": errors}}
                reporting_queue.put(error_report)
        elif output == "file":
            if output_format == "json":
                with open(output_path + table + ".log", "a") as f:
                    for record in records:
                        f.write(str(record) + "\n")
                    errors = {}
            elif output_format == "csv":
                with open(output_path + table + ".csv", "a") as f:
                    for record in records:
                        csv_record = ""
                        csv_record_position = 1
                        for current_table in schema["schema"]["tables"]:
                            if current_table["name"] == table:
                                csv_record_max_position = len(current_table["columns"])
                                if "id" in record:
                                    csv_record += str(record["id"]) + ","
                                for schema_column in sorted(
                                    current_table["columns"], key=itemgetter("name")
                                ):
                                    if schema_column["name"] in record:
                                        if (
                                            schema_column["type"]
                                            in ("multiple", "string", "text", "object")
                                            and len(record[schema_column["name"]]) > 0
                                        ):
                                            csv_record += (
                                                '"'
                                                + str(
                                                    record[schema_column["name"]]
                                                ).replace('"', '""')
                                                + '"'
                                            )
                                        else:
                                            csv_record += str(
                                                record[schema_column["name"]]
                                            )
                                    if csv_record_position < csv_record_max_position:
                                        csv_record += ","
                                    csv_record_position += 1
                        f.write(str(csv_record) + "\n")
                    errors = {}

        status_report = {}
        status_report = {table: {"records": len(records)}}
        reporting_queue.put(status_report)
        close_report = {table: {"records": None}}
        reporting_queue.put(close_report)
    elif mode == "full" and records_type == "with_links":
        if output == "xata" and len(records) > 0:
            resp, errors = post(
                apikey=to_XATA_API_KEY,
                urlPath=to_BRANCH_URL + "/tables/" + table + "/bulk",
                payload={"records": records},
                ERROR_FILE=ERROR_FILE,
                host_header=host_header,
            )
            if errors != {}:
                error_report = {}
                error_report = {table: {"errors": errors}}
                reporting_queue.put(error_report)
        status_report = {}
        status_report = {table: {"links": len(records)}}
        reporting_queue.put(status_report)
        close_report = {table: {"links": None}}
        reporting_queue.put(close_report)
    elif mode == "bulk_links_transaction":
        if output == "xata" and len(records) > 0:
            # transaction request
            transaction_payload = {}
            transaction_payload["operations"] = []
            for record_to_update in records:
                transaction_item = {}
                transaction_item["update"] = {}
                transaction_item["update"]["table"] = table
                transaction_item["update"]["id"] = record_to_update.pop("id")
                transaction_item["update"]["fields"] = record_to_update
                transaction_item["update"]["upsert"] = True
                transaction_payload["operations"].append(transaction_item)
            resp, errors = post(
                apikey=to_XATA_API_KEY,
                urlPath=to_BRANCH_URL + "/transaction",
                payload=transaction_payload,
                ERROR_FILE=ERROR_FILE,
                host_header=host_header,
            )
            if errors != {}:
                error_report = {}
                error_report = {table: {"errors": errors}}
                reporting_queue.put(error_report)
        status_report = {}
        status_report = {table: {"links": len(records)}}
        reporting_queue.put(status_report)
        close_report = {table: {"links": None}}
        reporting_queue.put(close_report)
    elif mode == "only_links":
        close_report = {table: {"links": None}}
        reporting_queue.put(close_report)


def reporter(
    queue,
    tables,
    table_categories,
    from_XATA_API_KEY,
    from_BRANCH_URL,
    ERROR_FILE,
    CONCURRENT_CONSUMERS,
    host_header,
    output,
    output_format,
    output_path,
):
    LINE_UP = "\033[1A"
    LINE_CLEAR = "\x1b[2K"
    report = {}
    table_status = {}
    if output == "xata":
        print("\nExecution plan:")
        for category in table_categories:
            if category == "category1" and len(table_categories[category]) > 0:
                print(
                    table_categories[category],
                    "do not contain links and will be copied first.",
                )
            if category == "category2" and len(table_categories[category]) > 0:
                print(
                    table_categories[category],
                    "contain links to other tables from the above list so will be copied second.",
                )
            if category == "category3" and len(table_categories[category]) > 0:
                print(
                    table_categories[category],
                    "contain links to other tables with links and will be copied last. Links will be backfilled after all records have been copied.",
                )
    elif output == "file":
        if output_format == "json":
            print("\nTable output file paths:")
            for category in table_categories:
                for ordered_table in table_categories[category]:
                    print(
                        "-", ordered_table + ":", output_path + ordered_table + ".log"
                    )
        elif output_format == "csv":
            print("\nTable output csv paths:")
            for category in table_categories:
                for ordered_table in table_categories[category]:
                    print(
                        "-", ordered_table + ":", output_path + ordered_table + ".csv"
                    )
    print("\n>>> COPYING TABLE DATA <<<\n")
    start = datetime.now()
    for table in tables:
        print("Initializing replay from", table)
        table_status[table] = {}
        table_status[table]["threads_finished"] = 0
        summaries, errors = post(
            apikey=from_XATA_API_KEY,
            urlPath=from_BRANCH_URL + "/tables/" + table + "/summarize",
            payload={"summaries": {"total": {"count": "*"}}},
            ERROR_FILE=ERROR_FILE,
            host_header=host_header,
        )
        table_status[table]["records_summary"] = summaries.json()["summaries"][0][
            "total"
        ]
        report[table] = {}
        report[table]["records"] = 0
        report[table]["errors"] = {}
        # Category 3 tables will also get links reporting due to individual link backfilling strategy
        if table in table_categories["category3"]:
            report[table]["links"] = 0
            table_status[table]["links_finished"] = 0
    while True:
        try:
            status_report = queue.get(block=False)
            for key in status_report:
                if "records" in status_report[key]:
                    if status_report[key]["records"] is not None:
                        report[key]["records"] += status_report[key]["records"]
                    else:
                        table_status[key]["threads_finished"] += 1
                if "links" in status_report[key]:
                    if status_report[key]["links"] is not None:
                        report[key]["links"] += status_report[key]["links"]
                    else:
                        table_status[key]["links_finished"] += 1
                if "errors" in status_report[key]:
                    # handle errors
                    for error_code in status_report[key]["errors"]:
                        if error_code in report[key]["errors"]:
                            report[key]["errors"][error_code] += status_report[key][
                                "errors"
                            ][error_code]
                        else:
                            report[key]["errors"][error_code] = status_report[key][
                                "errors"
                            ][error_code]
        except Empty:
            print(LINE_UP, end=LINE_CLEAR)
            print("Elapsed:", datetime.now() - start)
            sleep(0.1)
            continue

        print(LINE_UP, end=LINE_CLEAR)
        for table in report:
            print(LINE_UP, end=LINE_CLEAR)

        for table in report:
            if "records" in report[table] and "links" not in report[table]:
                if table_status[table]["threads_finished"] != CONCURRENT_CONSUMERS:
                    if report[table]["errors"] == {}:
                        print(
                            table,
                            ":",
                            report[table]["records"],
                            "/",
                            table_status[table]["records_summary"],
                        )
                    else:
                        print(
                            table,
                            ":",
                            report[table]["records"],
                            "/",
                            table_status[table]["records_summary"],
                            "Errors:",
                            report[table]["errors"],
                        )
                else:
                    if report[table]["errors"] == {}:
                        print(
                            table,
                            ":",
                            report[table]["records"],
                            "/",
                            table_status[table]["records_summary"],
                            "[Completed]",
                        )
                    else:
                        print(
                            table,
                            ":",
                            report[table]["records"],
                            "/",
                            table_status[table]["records_summary"],
                            "[Completed]",
                            "Errors:",
                            report[table]["errors"],
                        )
            if "records" in report[table] and "links" in report[table]:
                if report[table]["links"] > 0:
                    if (
                        table_status[table]["threads_finished"] == CONCURRENT_CONSUMERS
                        and table_status[table]["links_finished"]
                        == CONCURRENT_CONSUMERS
                    ):
                        if report[table]["errors"] == {}:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "[Completed]",
                                "| Backfilled links:",
                                report[table]["links"],
                                "[Completed]",
                            )
                        else:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "[Completed]",
                                "| Backfilled links:",
                                report[table]["links"],
                                "[Completed]",
                                "Errors:",
                                report[table]["errors"],
                            )
                    elif (
                        table_status[table]["threads_finished"] == CONCURRENT_CONSUMERS
                        and table_status[table]["links_finished"]
                        != CONCURRENT_CONSUMERS
                    ):
                        if report[table]["errors"] == {}:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "[Completed]",
                                "| Backfilled links:",
                                report[table]["links"],
                            )
                        else:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "[Completed]",
                                "| Backfilled links:",
                                report[table]["links"],
                                "Errors:",
                                report[table]["errors"],
                            )
                    elif (
                        table_status[table]["links_finished"] != CONCURRENT_CONSUMERS
                        and table_status[table]["threads_finished"]
                        != CONCURRENT_CONSUMERS
                    ):
                        if report[table]["errors"] == {}:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "| Backfilled links:",
                                report[table]["links"],
                            )
                        else:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "| Backfilled links:",
                                report[table]["links"],
                                "Errors:",
                                report[table]["errors"],
                            )
                else:
                    if table_status[table]["threads_finished"] != CONCURRENT_CONSUMERS:
                        if report[table]["errors"] == {}:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                            )
                        else:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "Errors:",
                                report[table]["errors"],
                            )
                    else:
                        if report[table]["errors"] == {}:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "[Completed]",
                            )
                        else:
                            print(
                                table,
                                ":",
                                report[table]["records"],
                                "/",
                                table_status[table]["records_summary"],
                                "[Completed]",
                                "Errors:",
                                report[table]["errors"],
                            )
        all_finished = True
        for table in tables:
            if "threads_finished" in table_status[table]:
                if table_status[table]["threads_finished"] != CONCURRENT_CONSUMERS:
                    all_finished = False
            # Only check for link completion when the output is Xata. When writting to file, links are written with a single pass, we do not backfill links,
            if ("links_finished" in table_status[table]) and output == "xata":
                if table_status[table]["links_finished"] != CONCURRENT_CONSUMERS:
                    all_finished = False
        print("Elapsed:", datetime.now() - start)
        if all_finished == True:
            records_sum = 0
            links_sum = 0
            suggest_check_errorlog = False
            for table in report:
                if "records" in report[table]:
                    records_sum += report[table]["records"]
                if "links" in report[table]:
                    links_sum += report[table]["links"]
                if "errors" in report[table]:
                    if report[table]["errors"] != {}:
                        suggest_check_errorlog = True
            if links_sum == 0:
                print(
                    "Data transfer completed. Processed", records_sum, "records total."
                )
            elif links_sum > 0:
                print(
                    "Data transfer completed. Processed",
                    records_sum,
                    "records total and backfilled",
                    links_sum,
                    "records with links.",
                )
            if suggest_check_errorlog == True:
                print("Check error log", ERROR_FILE, "for details of failed requests.")
            break
