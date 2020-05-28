CREATE TABLE [dbo].[stage_mms_ValAssessmentDay] (
    [stage_mms_ValAssessmentDay_id] BIGINT       NOT NULL,
    [ValAssessmentDayID]            SMALLINT     NULL,
    [Description]                   VARCHAR (50) NULL,
    [AssessmentDay]                 INT          NULL,
    [SortOrder]                     SMALLINT     NULL,
    [InsertedDatetime]              DATETIME     NULL,
    [UpdatedDateTime]               DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

