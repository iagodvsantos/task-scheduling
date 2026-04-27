CREATE KEYSPACE IF NOT EXISTS task_scheduling
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- processed is a clustering key so WHERE processed = false needs no ALLOW FILTERING.
-- "Marking processed" = atomic LOGGED BATCH: DELETE processed=false row +
-- INSERT processed=true row (preserves history) + INSERT next occurrence (processed=false).
CREATE TABLE IF NOT EXISTS task_scheduling.task_schedule (
    next_execution_time     bigint,
    segment                 bigint,
    processed               boolean,
    job_id                  bigint,
    priority                int,
    recurrence              bigint,
    original_execution_time bigint,
    PRIMARY KEY ((next_execution_time, segment), processed, job_id)
);

-- Per-task mutual exclusion during processing.
-- TTL auto-expires stale claims when a worker crashes mid-processing.
CREATE TABLE IF NOT EXISTS task_scheduling.task_in_flight (
    job_id               bigint,
    next_execution_time  bigint,
    segment              bigint,
    worker_id            text,
    PRIMARY KEY ((job_id, next_execution_time, segment))
) WITH default_time_to_live = 600;
