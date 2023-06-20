# Xreplay

Copy Xata databases across accounts, workspaces, regions and branches, or store schema and table content to files on disk in JSON or CSV format.

## Description

This script starts a cursor pagination scroll on each of the tables of the source database, stores inflight events in a memory queue and uses a configurable number of writer threads to emit the data to the the target database.

In order to orchestrate link creation, the script determines the best order for rewriting the table content.
Tables with no links are copied in a first pass, then tables with links to tables that don't have links are copied second in a second pass.
Tables with deep links are copied in a third pass, without carrying over the link ids - because we cannot safely tell if the record already exists yet in the linked table. Once indexing is completed, a next pass backfills the link ids.

The speed of the copy operation is largely dictated by the read speed of the scroll which is sequential and single threaded at the table level, however increasing the concurrency of writers does play a role in performance.

There are several different methods for the backfilling of links: bulk, atomic and transaction, which may yield significantly different performance. In the majority of cases the fastest method will be transaction, so it is used as the default.

## Usage

Run `xreplay.py` with python3.

## Arguments

Required:

- `--from_workspace`: the id of the workspace where the source database exists
- `--from_database`: the name of the source database
- `--from_branch`: the branch in the source database to copy from
- `--from_region`: the region of the source database
- `--from_XATA_API_KEY`: the API key to use for accessing the source workspace
- `--output`: one of "xata" (if copying to another Xata database) or "file" (if writing your database content to files on disk)

Required if writing to Xata:

- `--to_workspace`: the id of the workspace where we will copy the data to
- `--to_database`: the name of the target database. It must not exist, will be created by the tool.
- `--to_branch`: the branch of the target database to write to.
- `--to_region`: the region of the target database
- `--to_XATA_API_KEY`: the API key to use for accessing the target workspace

Optional:

- `--concurrency`: the number of write threads to use per table. 1 to 10. Default 2 and if writting to file it is automatically set to 1.
- `--bulk_size`: the number of records in the bulk write requests to the new database. 1 to 1000. Default 100.
- `--page_size`: the scroll page size to use when reading from the source database. 1 to 200. Default 200.
- `--queue_size`: the size of the in-memory queue for inflight events per table. page_size to 10000. Default 1000.
- `--output_path`: custom path on disk to write table content to, only if the file output is used.
- `--output_format`: File export format, must be one of `json` or `csv`.
- `--links_backfill_method`: link backfilling method. Can be one of bulk (which is the default), atomic, or transaction. Bulk will rewrite entire records when creating links but in bulks. Atomic will update only the link content, but it cannot be performed in bulk. Transaction performs bulk updates of links. Bulk will work faster in most cases, but the option for atomic backfill is available for cases with particularly large records where overwritting the entire record even in bulk, is slower than performing atomic updates. Lastly, transaction uses the experimental transaction api to perform link updates in bulks.
- `--custom_source`: Custom xata source url other than the production endpoint
- `--custom_source_host_header`: Custom host header to use with the custom source url
- `--custom_destination`: Custom xata destination url other than the production endpoint
- `--custom_destination_host_header`: Custom host header to use with the custom destination url
- `--custom_control_plane`: Control plane other than api.xata.io for managing the destination database

## Usage Examples

Writing to Xata without optional arguments:

```
python3 xreplay.py \
--from_workspace cp1jil \
--from_database mysourcedb \
--from_branch main \
--from_region eu-west-1 \
--from_XATA_API_KEY $SOURCE_XATA_API_KEY \
--to_workspace cp1jil \
--to_database mytargetdb \
--to_branch main \
--to_region eu-west-1 \
--to_XATA_API_KEY $DESTINATION_XATA_API_KEY \
--output xata
```

Writing to Xata with optional arguments:

```
python3 xreplay.py \
--from_workspace cp1jil \
--from_database mysourcedb \
--from_branch main \
--from_region eu-west-1 \
--from_XATA_API_KEY $SOURCE_XATA_API_KEY \
--to_workspace cp1jil \
--to_database mytargetdb \
--to_branch main \
--to_region eu-west-1 \
--to_XATA_API_KEY $DESTINATION_XATA_API_KEY \
--concurrency 5 \
--bulk_size 50 \
--page_size 100 \
--queue_size 2000 \
--output xata
```

Writing to file on disk in JSON format (default) under a custom directory (otherwise the default "output" is used):

```
python3 xreplay.py \
--from_workspace cp1jil \
--from_database mysourcedb \
--from_branch main \
--from_region eu-west-1 \
--from_XATA_API_KEY $SOURCE_XATA_API_KEY \
--output file \
--output_path myoutput
```

Writing to file on disk in CSV format under a custom directory:

```
python3 xreplay.py \
--from_workspace cp1jil \
--from_database mysourcedb \
--from_branch main \
--from_region eu-west-1 \
--from_XATA_API_KEY $SOURCE_XATA_API_KEY \
--output file \
--output_path myoutput \
--output_format csv
```

## Output samples

Copying data to another database in Xata:

```

python3 xreplay.py \
--from_workspace uq2d57 \
--from_database source \
--from_branch main \
--from_region eu-west-1 \
--from_XATA_API_KEY $FROM_XATA_API_KEY \
--to_workspace cp1jil \
--to_database target \
--to_branch main \
--to_region eu-west-1 \
--to_XATA_API_KEY $TO_XATA_API_KEY \
--output xata

>>> Xata Replay tool <<<

Using configuration:
 Concurrent writers per table: 2
 Bulk size: 100
 Page scroll size: 200
 Inflight records queue size: 1000

Applying schema:
- Retrieving schema from origin https://uq2d57.eu-west-1.xata.sh/db/source:main
- Creating schema in target https://cp1jil.eu-west-1.xata.sh/db/target:main
- Schema has been copied successfully

Execution plan:
['circuits', 'constructors', 'drivers', 'seasons', 'status'] do not contain links and will be copied first.
['races'] contain links to other tables from the above list so will be copied second.
['qualifying', 'pitStops', 'constructorStandings', 'constructorResults', 'lapTimes', 'driverStandings', 'results', 'sprintResults'] contain links to other tables with links and will be copied last. Links will be backfilled after all records have been copied.

>>> TRANSFERING TABLE DATA <<<
circuits : 76 / 76 [Completed]
constructors : 211 / 211 [Completed]
drivers : 854 / 854 [Completed]
seasons : 73 / 73 [Completed]
status : 139 / 139 [Completed]
races : 1079 / 1079 [Completed]
qualifying : 9395 / 9395 [Completed] | Backfilled links: 9395 [Completed]
pitStops : 9299 / 9299 [Completed] | Backfilled links: 9299 [Completed]
constructorStandings : 12841 / 12841 [Completed] | Backfilled links: 12841 [Completed]
constructorResults : 12080 / 12080 [Completed] | Backfilled links: 12080 [Completed]
lapTimes : 14800 / 14800 [Completed] | Backfilled links: 14800 [Completed]
driverStandings : 15000 / 15000 [Completed] | Backfilled links: 15000 [Completed]
results : 0 / 0 [Completed]
sprintResults : 100 / 100 [Completed] | Backfilled links: 100 [Completed]
Elapsed: 0:45:19.053440
Data transfer completed. Copied 75947 records total and backfilled 73515 link records.
```

Copying data to another database in Xata while throttling errors are encountered (and automatically retried):

```
python3 xreplay.py --from_workspace cp1jil \
--from_database f1source \
--from_branch main \
--from_region eu-west-1 \
--from_XATA_API_KEY $FROM_XATA_API_KEY \
--to_workspace cp1jil \
--to_database f1target \
--to_branch main \
--to_region eu-west-1 \
--to_XATA_API_KEY $TO_XATA_API_KEY \
--bulk_size 200 \
--concurrency 4 \
--output xata

>>> Xata Replay tool <<<

Using configuration:
 Concurrent writers per table: 4
 Bulk size: 200
 Page scroll size: 200
 Inflight records queue size: 1000
 Error logs written to: logs/debug-cp1jil-f1target-main-2023-01-30T19:25:54.log

Applying schema:
- Retrieving schema from origin https://cp1jil.eu-west-1.xata.sh/db/f1source:main
- Creating schema in target https://cp1jil.eu-west-1.xata.sh/db/f1target:main
- Schema has been copied successfully

Execution plan:
['circuits', 'constructors', 'drivers', 'seasons', 'status'] do not contain links and will be copied first.
['races'] contain links to other tables from the above list so will be copied second.
['qualifying', 'pitStops', 'constructorStandings', 'constructorResults', 'lapTimes', 'driverStandings', 'results', 'sprintResults'] contain links to other tables with links and will be copied last. Links will be backfilled after all records have been copied.

>>> COPYING TABLE DATA <<<
status : 139 / 139 [Completed]
races : 1079 / 1079 [Completed]
circuits : 76 / 76 [Completed]
constructors : 211 / 211 [Completed]
drivers : 854 / 854 [Completed]
seasons : 73 / 73 [Completed]
status : 139 / 139 [Completed]
races : 1079 / 1079 [Completed]
qualifying : 9395 / 9395 [Completed] | Backfilled links: 1105 Errors: {429: 7}
pitStops : 9299 / 9299 [Completed] | Backfilled links: 1096 Errors: {429: 20}
constructorStandings : 12841 / 12841 [Completed] | Backfilled links: 1102 Errors: {429: 11}
constructorResults : 12080 / 12080 [Completed] | Backfilled links: 1104 Errors: {429: 6}
lapTimes : 46000 / 46000 [Completed] | Backfilled links: 1103 Errors: {429: 12}
driverStandings : 33686 / 33686 [Completed] | Backfilled links: 1111 Errors: {429: 6}
results : 0 / 0 [Completed]
sprintResults : 100 / 100 [Completed] | Backfilled links: 100 [Completed] Errors: {429: 9}
Elapsed: 0:03:11.914233
```

Copying data to files on disk:

```
python3 xreplay.py --from_workspace cp1jil \
--from_database source \
--from_branch main \
--from_region eu-west-1 \
--from_XATA_API_KEY $FROM_XATA_API_KEY \
--output file \
--output_path mydata

>>> Xata Replay tool <<<

Using configuration:
 Concurrent writers per table: 1
 Bulk size: 100
 Page scroll size: 200
 Inflight records queue size: 1000
 Error logs written to: logs/debug-cp1jil-source-main-2023-01-30T19:13:20.log

Applying schema:
- Retrieving schema from origin https://cp1jil.eu-west-1.xata.sh/db/target:main
- Writing schema to my/schema.json

Table output file paths:
- workspaces: mydata/workspaces.log
- users: mydata/users.log
- data_integrity: mydata/data_integrity.log
- databases: mydata/databases.log

>>> COPYING TABLE DATA <<<
workspaces : 4892 / 4892 [Completed]
databases : 5562 / 5562 [Completed]
users : 3962 / 3962 [Completed]
data_integrity : 9 / 9 [Completed]
Elapsed: 0:00:25.065605
Data transfer completed. Processed 14425 records total.
```

## Converting file outputs to CSV

You can use the `--output_format csv` parameter combined with `--output file` to export records to CSV.

Additionally, the following script can be used to convert JSON records to CSV format.

```
import pandas as pd
file = open('outputfile.log', 'r')
lines = file.readlines()
for line in lines:
     mydict=eval(line)
     df = pd.DataFrame.from_dict([mydict])
     df.to_csv (r'outputfile.csv', index=False, header=False, mode='a')
```
