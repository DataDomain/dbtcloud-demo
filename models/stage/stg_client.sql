WITH client AS (

    SELECT DISTINCT job_client AS client_name
    FROM {{ source( "SF_source", 'DATADOMAIN_TIMESHEETS_WFM_EXTRACT' )}}

),

final AS (

    SELECT 
    client_name
    FROM client

)

SELECT * FROM final