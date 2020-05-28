CREATE TABLE [dbo].[d_mms_membership_balance_snapshot] (
    [d_mms_membership_balance_snapshot_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [dim_mms_membership_key]               CHAR (32)       NULL,
    [membership_id]                        INT             NULL,
    [committed_balance_products]           DECIMAL (26, 6) NULL,
    [current_balance_products]             DECIMAL (26, 6) NULL,
    [end_of_day_committed_balance]         DECIMAL (26, 6) NULL,
    [end_of_day_current_balance]           DECIMAL (26, 6) NULL,
    [end_of_day_statement_balance]         DECIMAL (26, 6) NULL,
    [membership_balance_id]                INT             NULL,
    [processing_complete_flag]             CHAR (10)       NULL,
    [p_mms_membership_balance_snapshot_id] BIGINT          NOT NULL,
    [dv_load_date_time]                    DATETIME        NULL,
    [dv_load_end_date_time]                DATETIME        NULL,
    [dv_batch_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_membership_balance_snapshot]([dv_batch_id] ASC);

