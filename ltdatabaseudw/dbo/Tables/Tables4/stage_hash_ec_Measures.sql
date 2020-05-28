CREATE TABLE [dbo].[stage_hash_ec_Measures] (
    [stage_hash_ec_Measures_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [MeasuresId]                      VARCHAR (36)    NULL,
    [Slug]                            NVARCHAR (100)  NULL,
    [Title]                           NVARCHAR (100)  NULL,
    [Tags]                            NVARCHAR (4000) NULL,
    [Description]                     NVARCHAR (500)  NULL,
    [Unit]                            NVARCHAR (100)  NULL,
    [MeasureValueType]                INT             NULL,
    [ExtendedMetadata]                NVARCHAR (4000) NULL,
    [Gender]                          NVARCHAR (10)   NULL,
    [OptimumRangeMale]                NVARCHAR (100)  NULL,
    [OptimumRangeFemale]              NVARCHAR (100)  NULL,
    [DiagonosticRangeMale]            NVARCHAR (100)  NULL,
    [DiagonosticRangeFemale]          NVARCHAR (100)  NULL,
    [CreatedBy]                       INT             NULL,
    [CreatedDate]                     DATETIME        NULL,
    [ModifiedBy]                      INT             NULL,
    [ModifiedDate]                    DATETIME        NULL,
    [MeasurementType]                 INT             NULL,
    [MeasurementInstructionsLocation] NVARCHAR (200)  NULL,
    [dv_load_date_time]               DATETIME        NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

