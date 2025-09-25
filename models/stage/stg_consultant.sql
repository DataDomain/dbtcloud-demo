WITH consultants AS (
    
    SELECT DISTINCT staff_name AS consultant_name
    FROM RAW.DATADOMAIN_TIMESHEETS_WFM_EXTRACT

),

contractors AS (

    SELECT
    SUBSTRING( consultant_name, LEN('z-C - ') + 1 ) AS consultant_name,
    --apply manual fix to Hennie showing as contractor in WFM
    CASE WHEN consultant_name = 'z-C - Hennie Steyn' THEN 0 ELSE 1 END AS contractor_flag
    FROM consultants
    WHERE POSITION( 'z-C - ' IN consultant_name ) = 1

),

non_contractors AS (

    SELECT
    consultant_name,
    0 AS contractor_flag
    FROM consultants
    WHERE POSITION( 'z-C - ' in consultant_name ) = 0

),

final AS (

    SELECT * FROM contractors
    UNION
    SELECT * FROM non_contractors

)

SELECT * FROM final