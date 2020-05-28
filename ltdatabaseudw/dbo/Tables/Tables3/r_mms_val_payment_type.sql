CREATE TABLE [dbo].[r_mms_val_payment_type] (
    [r_mms_val_payment_type_id]      BIGINT       NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [val_payment_type_id]            INT          NULL,
    [description]                    VARCHAR (50) NULL,
    [sort_order]                     INT          NULL,
    [val_eft_account_type_id]        INT          NULL,
    [view_payment_type_flag]         BIT          NULL,
    [view_bank_account_type_flag]    BIT          NULL,
    [inserted_date_time]             DATETIME     NULL,
    [updated_date_time]              DATETIME     NULL,
    [requires_payment_terminal_flag] BIT          NULL,
    [dv_load_date_time]              DATETIME     NOT NULL,
    [dv_load_end_date_time]          DATETIME     NOT NULL,
    [dv_batch_id]                    BIGINT       NOT NULL,
    [dv_r_load_source_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL,
    [dv_hash]                        CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([r_mms_val_payment_type_id]));

