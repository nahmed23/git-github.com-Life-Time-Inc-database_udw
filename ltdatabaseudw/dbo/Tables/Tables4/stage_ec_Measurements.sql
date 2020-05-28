CREATE TABLE [dbo].[stage_ec_Measurements] (
    [stage_ec_Measurements_id] BIGINT          NOT NULL,
    [MeasurementId]            VARCHAR (36)    NULL,
    [MeasurementRecordingId]   VARCHAR (36)    NULL,
    [MeasureValue]             NVARCHAR (100)  NULL,
    [MeasuresId]               VARCHAR (36)    NULL,
    [Unit]                     NVARCHAR (4000) NULL,
    [jan_one]                  DATETIME        NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

