WITH tasks AS (
    
    SELECT DISTINCT 

        job_no AS project_no,
        --clean duplicates due to inconsistent hyphen format        
        UPPER(TRIM(REPLACE(
                REPLACE(
                    REPLACE(
                        job_task_label,
                        '–', '-'     -- en dash to hyphen
                    ),
                    '—', '-'         -- em dash to hyphen
                ),
                '−', '-'             -- minus sign to hyphen
            ))) AS task_label,
        job_task_name AS task_category,
        time_billable AS task_billable,
        job_task_remaining_time AS task_remaining_time,
        job_task_actual_time AS task_actual_time,
        job_task_estimated_time AS task_estimated_time
    FROM {{ source( "SF_source", 'DATADOMAIN_TIMESHEETS_WFM_EXTRACT' )}}

),

final AS (

    SELECT 

        project_no,
        --prevent empty task_label as task_label is a natural key?
        --CASE WHEN task_label = "" THEN "Unknown task" ELSE task_label END AS task_label,
        task_label,
        task_category,
        task_billable,
        task_remaining_time,
        task_actual_time,
        task_estimated_time

    FROM tasks

)

SELECT * FROM final