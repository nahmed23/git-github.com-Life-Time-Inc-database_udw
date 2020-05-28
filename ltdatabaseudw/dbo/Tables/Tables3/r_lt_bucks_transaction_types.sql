CREATE TABLE [dbo].[r_lt_bucks_transaction_types] (
    [r_lt_bucks_transaction_types_id] BIGINT       NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [ttype_id]                        INT          NULL,
    [ttype_desc]                      VARCHAR (50) NULL,
    [last_modified_timestamp]         DATETIME     NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_load_end_date_time]           DATETIME     NOT NULL,
    [dv_batch_id]                     BIGINT       NOT NULL,
    [dv_r_load_source_id]             BIGINT       NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_hash]                         CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([r_lt_bucks_transaction_types_id]));

