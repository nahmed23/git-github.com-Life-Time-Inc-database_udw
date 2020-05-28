CREATE TABLE [dbo].[s_exacttarget_multiple_data_extension_send_lists] (
    [s_exacttarget_multiple_data_extension_send_lists_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                             CHAR (32)      NOT NULL,
    [client_id]                                           BIGINT         NULL,
    [send_id]                                             BIGINT         NULL,
    [list_id]                                             BIGINT         NULL,
    [data_extension_name]                                 VARCHAR (4000) NULL,
    [status]                                              VARCHAR (4000) NULL,
    [date_created]                                        DATETIME       NULL,
    [de_client_id]                                        BIGINT         NULL,
    [jan_one]                                             DATETIME       NULL,
    [dv_load_date_time]                                   DATETIME       NOT NULL,
    [dv_batch_id]                                         BIGINT         NOT NULL,
    [dv_r_load_source_id]                                 BIGINT         NOT NULL,
    [dv_inserted_date_time]                               DATETIME       NOT NULL,
    [dv_insert_user]                                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                                DATETIME       NULL,
    [dv_update_user]                                      VARCHAR (50)   NULL,
    [dv_hash]                                             CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

