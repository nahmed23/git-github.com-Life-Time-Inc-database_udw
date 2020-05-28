CREATE TABLE [dbo].[s_mms_package] (
    [s_mms_package_id]            BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [package_id]                  INT             NULL,
    [number_of_sessions]          SMALLINT        NULL,
    [price_per_session]           NUMERIC (9, 4)  NULL,
    [created_date_time]           DATETIME        NULL,
    [utc_created_date_time]       DATETIME        NULL,
    [created_date_time_zone]      VARCHAR (4)     NULL,
    [sessions_left]               SMALLINT        NULL,
    [balance_amount]              NUMERIC (9, 4)  NULL,
    [inserted_date_time]          DATETIME        NULL,
    [updated_date_time]           DATETIME        NULL,
    [package_edited_flag]         BIT             NULL,
    [package_edit_date_time]      DATETIME        NULL,
    [utc_package_edit_date_time]  DATETIME        NULL,
    [package_edit_date_time_zone] VARCHAR (4)     NULL,
    [expiration_date_time]        DATETIME        NULL,
    [unexpire_count]              INT             NULL,
    [last_unexpired_date_time]    DATETIME        NULL,
    [transaction_source]          VARCHAR (50)    NULL,
    [original_balance_amount]     DECIMAL (26, 6) NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL,
    [dv_deleted]                  BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_package]([dv_batch_id] ASC);

