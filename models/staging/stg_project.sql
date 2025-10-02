WITH projects AS (
    
    SELECT DISTINCT 
        job_name AS project_name,
        job_milestones AS project_milestones,
        job_milestones_completed AS project_milestones_completed,
        job_no AS project_no,
        job_manager AS project_manager,
        job_category AS project_category,
        job_description AS project_description,
        job_start_date AS project_start_date,
        job_due_date AS project_due_date,
        job_client_order_no AS project_client_order_no
    FROM {{ source( "SF_source", 'DATADOMAIN_TIMESHEETS_WFM_EXTRACT' )}}

),

final AS (

    SELECT 
        project_name,
        project_no,
        project_manager,
        project_category,
        project_description,
        project_start_date,
        project_due_date,
        project_client_order_no,
        project_milestones,
        project_milestones_completed
    FROM projects

)

SELECT * FROM final