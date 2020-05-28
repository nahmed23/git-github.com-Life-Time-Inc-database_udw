CREATE TABLE [dbo].[d_mms_email_address_status] (
    [d_mms_email_address_status_id]           BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)     NOT NULL,
    [dim_mms_email_address_status_key]        CHAR (32)     NULL,
    [email_address_status_id]                 INT           NULL,
    [email_address]                           VARCHAR (140) NULL,
    [email_address_search]                    VARCHAR (10)  NULL,
    [status_from_date]                        DATETIME      NULL,
    [status_from_dim_date_key]                CHAR (8)      NULL,
    [status_from_dim_time_key]                CHAR (8)      NULL,
    [status_thru_date]                        DATETIME      NULL,
    [status_thru_dim_date_key]                CHAR (8)      NULL,
    [status_thru_dim_time_key]                CHAR (8)      NULL,
    [val_communication_preference_source_key] VARCHAR (255) NULL,
    [val_communication_preference_status_key] VARCHAR (255) NULL,
    [p_mms_email_address_status_id]           BIGINT        NOT NULL,
    [deleted_flag]                            INT           NULL,
    [dv_load_date_time]                       DATETIME      NULL,
    [dv_load_end_date_time]                   DATETIME      NULL,
    [dv_batch_id]                             BIGINT        NOT NULL,
    [dv_inserted_date_time]                   DATETIME      NOT NULL,
    [dv_insert_user]                          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                    DATETIME      NULL,
    [dv_update_user]                          VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_email_address_status]([dv_batch_id] ASC);

