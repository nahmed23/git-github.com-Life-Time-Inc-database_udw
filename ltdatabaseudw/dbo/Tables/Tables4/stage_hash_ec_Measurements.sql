CREATE TABLE [dbo].[stage_hash_ec_Measurements] (
    [stage_hash_ec_Measurements_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [MeasurementId]                 VARCHAR (36)    NULL,
    [MeasurementRecordingId]        VARCHAR (36)    NULL,
    [MeasureValue]                  NVARCHAR (100)  NULL,
    [MeasuresId]                    VARCHAR (36)    NULL,
    [Unit]                          NVARCHAR (4000) NULL,
    [jan_one]                       DATETIME        NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_updated_date_time]          DATETIME        NULL,
    [dv_update_user]                VARCHAR (50)    NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

