
/*********
PLAUSIBLE_TEMPORAL_VALUE
Check that (non-NULL) timestamps do not occur exactly at midnight (i.e. that the time part of the entry is not 00:00:00.000000).
While this is a possible value, it is likely to indicate that the actual time of occurrence is not known
Indeed, the official ETL guidance for datetime fields in the OMOP CDM states that if the time is not known, setting it to 00:00:0000 is the standard convention.
The presence of midnight timestamps is thus not prohibited, but it can still indicate that timestamps are not reliable beyond the date.
The point of this check is therefore to flag tables in which many timestamps appear unreliable in this manner.
Denominator is number of events with a non-null datetime.

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
{@cohort & '@runForCohort' == 'Yes'}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
}
**********/

SELECT
    num_violated_rows,
    CASE
        WHEN denominator.num_rows = 0 THEN 0
        ELSE 1.0*num_violated_rows/denominator.num_rows
    END AS pct_violated_rows,
    denominator.num_rows AS num_denominator_rows
FROM
(
    SELECT
        COUNT_BIG(violated_rows.violating_field) AS num_violated_rows
    FROM
        (
            /*violatedRowsBegin*/
            SELECT
                '@cdmTableName.@cdmFieldName' AS violating_field,
                cdmTable.*
            FROM @cdmDatabaseSchema.@cdmTableName cdmTable
                {@cohort & '@runForCohort' == 'Yes'}?{
                JOIN @cohortDatabaseSchema.@cohortTableName c
                    ON cdmTable.person_id = c.subject_id
                    AND c.COHORT_DEFINITION_ID = @cohortDefinitionId
                }
            WHERE cdmTable.@cdmFieldName IS NOT NULL AND CAST(cdmTable.@cdmFieldName AS TIME) = '00:00:00.000000'
            /*violatedRowsEnd*/
        ) violated_rows
) violated_row_count,
(
    SELECT
        COUNT_BIG(*) AS num_rows
    FROM @cdmDatabaseSchema.@cdmTableName cdmTable
    {@cohort & '@runForCohort' == 'Yes'}?{
    JOIN @cohortDatabaseSchema.@cohortTableName c
        ON cdmTable.person_id = c.subject_id
        AND c.cohort_definition_id = @cohortDefinitionId
    }
    WHERE cdmTable.@cdmFieldName IS NOT NULL
) denominator
;
