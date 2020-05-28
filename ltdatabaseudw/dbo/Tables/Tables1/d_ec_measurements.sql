CREATE TABLE [dbo].[d_ec_measurements] (
    [d_ec_measurements_id]                BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [fact_trainerize_measurement_key]     VARCHAR (32)    NULL,
    [measurement_id]                      VARCHAR (36)    NULL,
    [d_ec_measurement_recordings_bk_hash] VARCHAR (32)    NULL,
    [dim_trainerize_measure_key]          VARCHAR (32)    NULL,
    [measure_value]                       NVARCHAR (100)  NULL,
    [unit]                                NVARCHAR (4000) NULL,
    [p_ec_measurements_id]                BIGINT          NOT NULL,
    [deleted_flag]                        INT             NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

