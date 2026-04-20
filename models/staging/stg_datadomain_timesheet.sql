WITH source1 AS (
    
    SELECT
    *
    FROM {{ source( "SF_source", 'DATADOMAIN_TIMESHEETS_2025_01_ORIGINAL' )}}

),

source2 AS (

    SELECT
    *
    FROM {{ source( "SF_source", 'DATADOMAIN_TIMESHEETS_2025_2_ORIGINAL' )}}

),

merge AS (
    SELECT
        ACTUALTIME as actual_time,
        BILLABLE,
        CATEGORY,
        CLIENT,
        DATE,
        DESCRIPTION,
        DUEDATE as due_date,
        ESTIMATEDTIME as estimated_time,
        LABEL as task_label,
        PROJECTSTAFF,
        null as percent_complete,
        REMAININGTIME as remaining_time,
        TASKSTAFFNAME as staff_name,
        null as start_date,
        null as JOB_MANAGER,
        null as task_date,
        null as task_name,
        TASKSTATUS as task_status,
        TIME as task_time,
        null as task_time_totalled,
        TASKTYPENAME as task_type_name,
        'DATADOMAIN_TIMESHEETS_2025_01_ORIGINAL' as SOURCE
    FROM source1
    UNION
    SELECT
        ACTUALTIME as actual_time,
        BILLABLE,
        CATEGORY,
        CLIENT,
        null as DATE,
        DESCRIPTION,
        DUEDATE as due_date,
        ESTIMATEDTIME as estimated_time,
        LABEL as task_label,
        null as projectsstaff,
        PERCENTCOMPLETE as percent_complete,
        REMAININGTIME as remaining_time,
        STAFFNAME as staff_name,
        STARTDATE as start_date,
        JOBMANAGER as JOB_MANAGER,
        TASKDATE as task_date,
        TASKNAME as task_name,
        null as task_status,
        TASKTIME as task_time, 
        TASKTIMETOTALLED as task_time_totalled,
        TASKTYPENAME as task_type_name,
        'DATADOMAIN_TIMESHEETS_2025_02_ORIGINAL' as SOURCE
    FROM source2

)

select * from merge