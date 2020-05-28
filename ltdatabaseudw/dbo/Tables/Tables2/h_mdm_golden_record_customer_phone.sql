CREATE TABLE [dbo].[h_mdm_golden_record_customer_phone] (
    [h_mdm_golden_record_customer_phone_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)      NOT NULL,
    [phone_type]                            CHAR (40)      NULL,
    [phone]                                 CHAR (40)      NULL,
    [entity_id]                             NVARCHAR (128) NULL,
    [dv_load_date_time]                     DATETIME       NOT NULL,
    [dv_r_load_source_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]                 DATETIME       NOT NULL,
    [dv_insert_user]                        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                  DATETIME       NULL,
    [dv_update_user]                        VARCHAR (50)   NULL,
    [dv_deleted]                            BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

