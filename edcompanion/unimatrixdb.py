##      ## ##     ## ##    ##  ######   #######  ########
##  ##  ## ##     ##  ##  ##  ##    ## ##     ## ##     ##
##  ##  ## ##     ##   ####   ##       ##     ## ##     ##
##  ##  ## #########    ##     ######  ##     ## ########
##  ##  ## ##     ##    ##          ## ##     ## ##   ##
##  ##  ## ##     ##    ##    ##    ## ##     ## ##    ##
 ###  ###  ##     ##    ##     ######   #######  ##     ##

# (C) 2020 Whysor B.V.


#%% -------------------------------------------------------
import collections
import typing
import uuid
import arrow
import pg8000 as dbapi2

from warpcore.manifold import Sample, SampleData, TaskData, ProcessMessage

# pylint: disable=broad-except
# pylint: disable=invalid-name

#%% *************************************************

#%% **********************************************
class DBUnimatrix(typing.NamedTuple):
    '''
    Immutable class and interface to the PostgreSQL task queue

    connection: dbapi2 connection object
    execute(operation, *args): execute an sql query with parameters
    commit(): connection.commit()
    insert_process(model_id, worker_id): assign a worker to handle assigned tasks against model.process
    insert_task(model_id, Task, time): insert a Task for a model
    fetch_tasks(): fetch unscheduled tasks for this dispatch_id

    Simplified entity diagram:
    +-------------+     +-------------+
    | model_id    |>---1| model_id    |
    | time/status |     | worker_id   |
    | Sample      |     | dispatch_id |
    +-------------+     +-------------+
    '''
    connection: dbapi2.Connection
    execute: typing.Callable
    commit: typing.Callable
    assign_process: typing.Callable
    insert_task: typing.Callable
    fetch_tasks: typing.Callable
    search_tasks: typing.Callable
    complete_task: typing.Callable
    calc_workload: typing.Callable
    count_workload: typing.Callable

ProcessingJob = collections.namedtuple(
    "ProcessingJob",
    ["worker_id", "job"])


DBTableDefinition = collections.namedtuple(
    "DBTableDefinition", ["name", "columns"]
)
DBColumnDefinition = collections.namedtuple(
    "DBColumnDefinition",
    ["name", "type", "index"],
    defaults=[False]
)
DBIndexDefinition = collections.namedtuple(
    "DBIndexDefinition", ["name", "table", "indexdef"]
)

def init_database(url, puid=str(uuid.uuid4())):
    # pylint: disable=too-many-locals
    version = "01"
    connection = _create_connection(url)
    cursor = connection.cursor()

    tasks = "tasks" + "_" + version
    processes = "workers" + "_" + version

    # execute an sql-query
    def _execute(operation, *args):
        cursor.execute(operation, *args)

    # generic iterator function for select queries result sets
    def _select(operation, *args):
        cursor.execute(operation, *args)
        while True:
            subset = cursor.fetchmany()
            if not subset:
                break
            for row in subset:
                yield row

    def _create_table(table_def):
        try:
            qs = "CREATE TABLE IF NOT EXISTS " + table_def.name + "(" + ",".join([" ".join(D[0:2]) for D in table_def.columns]) + ")"
            print(qs)
            cursor.execute(qs)

        except dbapi2.ProgrammingError:
            pass # Don need to see that it already exists
        finally:
            connection.commit()

        for column in table_def.columns:
            if column.index:
                try:
                    qs = "CREATE INDEX " + table_def.name + "_"  + column.name + "_idx ON " + table_def.name
                    if column.type in ["UUID"]:
                        qs += " USING HASH"
                    qs += "(" + column.name + ")"
                    print(qs)
                    cursor.execute(qs)
                except dbapi2.ProgrammingError:
                    pass # Don need to see that it already exists
                finally:
                    connection.commit()

        return table_def
    # ---------------------------

    # matrix processes
    _create_table(DBTableDefinition(processes, [
        DBColumnDefinition("model_id", "UUID UNIQUE", True),    # model
        DBColumnDefinition("dispatch_id", "UUID", True),        # instance id
        DBColumnDefinition("worker_id", "UUID", True)
    ]))
    # intake matrix
    _create_table(DBTableDefinition(tasks, [
        DBColumnDefinition("model_id", "UUID NOT NULL", True),  # model that is to process this sample
        DBColumnDefinition("job_id", "UUID", True),  # model that is to process this sample
        DBColumnDefinition("time", "BIGINT", True),             # timeframe of insertion
        DBColumnDefinition("scheduled", "BIGINT", True), # scheduled
        DBColumnDefinition("completed", "BIGINT", True),        # completion timeframe
        DBColumnDefinition("sample_type", "VARCHAR(8)"),        # Sample
        DBColumnDefinition("sensor_id", "UUID"),                #   |
        DBColumnDefinition("sample_time", "DOUBLE PRECISION"),  #   |
        DBColumnDefinition("sample_value", "DOUBLE PRECISION")  #   |
    ]))

    qry_assign_process = "INSERT INTO " + processes + "(dispatch_id, worker_id, model_id) " \
        "VALUES(%s,%s,%s) ON CONFLICT (model_id) DO UPDATE SET " \
        "dispatch_id = EXCLUDED.dispatch_id, worker_id=EXCLUDED.worker_id"
    qry_insert_task = "INSERT INTO " + tasks + " (sensor_id, model_id, time, sample_type, sample_time, sample_value) " \
        "VALUES(%s,%s,%s,%s,%s,%s)"
    qry_schedule_tasks = "UPDATE " + tasks + " SET job_id = %s, scheduled = %s" \
        "FROM " + processes + " WHERE dispatch_id = %s AND job_id IS NULL"
    qry_fetch_tasks = "SELECT worker_id, model_id, sample_type, sensor_id, sample_time, sample_value " \
        "FROM " + tasks + " NATURAL INNER JOIN " + processes + " WHERE "\
        "job_id = %s ORDER BY time"
    qry_complete_task = "UPDATE " + tasks + " SET completed = %s "\
        "FROM " + processes + " WHERE dispatch_id = %s and "+tasks+".model_id = %s"
    qry_search_task = "SELECT model_id, count(time) FROM " + tasks + " NATURAL LEFT JOIN " + processes + " WHERE "\
        "dispatch_id IS NULL GROUP BY model_id ORDER BY count(time) DESC LIMIT 1"
    qry_calc_workload = "SELECT worker_id, sum(completed - scheduled)/count(1) as total_time FROM " + tasks + " NATURAL INNER JOIN " + processes + " WHERE "\
        "dispatch_id = %s AND completed IS NOT NULL AND completed > %s"\
        "GROUP BY worker_id ORDER BY total_time"
    qry_count_workload = "SELECT count(model_id) FROM " + processes + " WHERE dispatch_id = %s GROUP BY worker_id ORDER BY count(model_id)"

    def _fetch_tasks():
        job_id = uuid.uuid4()
        _execute(qry_schedule_tasks, (job_id, int(arrow.utcnow().timestamp), uuid.UUID(puid)))
        connection.commit()
        for row in _select(qry_fetch_tasks, (job_id,)):
            yield ProcessingJob(
                row[0], ProcessMessage(
                    row[1].hex,
                    TaskData(
                        row[2],
                        [SampleData(row[3].hex, [Sample(row[4], row[5])])]
                    )
                )
            )

    return DBUnimatrix(
        connection=connection,
        execute=_execute,
        commit=connection.commit,
        assign_process=lambda M, W: _execute(
            qry_assign_process,
            (uuid.UUID(puid), uuid.UUID(W), uuid.UUID(M))),
        insert_task=lambda Model, Task, t: _execute(
            qry_insert_task,
            (uuid.UUID(Task.sample_data.sensor_id), uuid.UUID(Model), int(t), Task.sample_type, Task.sample_data.samples.time, Task.sample_data.samples.value)),
        fetch_tasks=_fetch_tasks,
        search_tasks=lambda t: _select(qry_search_task),
        complete_task=lambda M: _execute(qry_complete_task, (int(arrow.utcnow().timestamp), uuid.UUID(puid), uuid.UUID(M))),
        calc_workload=lambda t: _select(qry_calc_workload, (uuid.UUID(puid), int(t))),
        count_workload=lambda: _select("", (uuid.UUID(puid),))
    )

# **********************************************
def _create_connection(url):
    protocol, userp, pass_host, port_path = url.split(":")
    pwd, host = pass_host.split("@")
    port, path = port_path.split("/")
    connect_args = {
        "user": userp.replace("//", ""),
        "password": pwd,
        "host": host,
        "port": int(port)
    }
    if "?" in path:
        database, options = path.split("?")
        connect_args["database"] = database
        connect_args["options"] = options
    else:
        connect_args["database"] = path

    return dbapi2.connect(**connect_args)

