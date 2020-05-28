CREATE TABLE [dbo].[s_ec_measurement_recordings] (
    [s_ec_measurement_recordings_id] BIGINT             IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)          NOT NULL,
    [measurement_recording_id]       VARCHAR (36)       NULL,
    [measure_date]                   DATETIMEOFFSET (7) NULL,
    [notes]                          NVARCHAR (500)     NULL,
    [source]                         NVARCHAR (100)     NULL,
    [active]                         BIT                NULL,
    [certified]                      BIT                NULL,
    [created_by]                     INT                NULL,
    [created_date]                   DATETIMEOFFSET (7) NULL,
    [modified_by]                    INT                NULL,
    [modified_date]                  DATETIMEOFFSET (7) NULL,
    [metadata]                       NVARCHAR (4000)    NULL,
    [dv_load_date_time]              DATETIME           NOT NULL,
    [dv_r_load_source_id]            BIGINT             NOT NULL,
    [dv_inserted_date_time]          DATETIME           NOT NULL,
    [dv_insert_user]                 VARCHAR (50)       NOT NULL,
    [dv_updated_date_time]           DATETIME           NULL,
    [dv_update_user]                 VARCHAR (50)       NULL,
    [dv_hash]                        CHAR (32)          NOT NULL,
    [dv_batch_id]                    BIGINT             NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ec_measurement_recordings]([dv_batch_id] ASC);

