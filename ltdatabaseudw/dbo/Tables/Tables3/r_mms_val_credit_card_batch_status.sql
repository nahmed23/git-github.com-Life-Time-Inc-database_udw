CREATE TABLE [dbo].[r_mms_val_credit_card_batch_status] (
    [r_mms_val_credit_card_batch_status_id] BIGINT       NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [val_credit_card_batch_status_id]       SMALLINT     NULL,
    [description]                           VARCHAR (50) NULL,
    [sort_order]                            SMALLINT     NULL,
    [inserted_date_time]                    DATETIME     NULL,
    [updated_date_time]                     DATETIME     NULL,
    [dv_load_date_time]                     DATETIME     NOT NULL,
    [dv_load_end_date_time]                 DATETIME     NOT NULL,
    [dv_batch_id]                           BIGINT       NOT NULL,
    [dv_r_load_source_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL,
    [dv_hash]                               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

