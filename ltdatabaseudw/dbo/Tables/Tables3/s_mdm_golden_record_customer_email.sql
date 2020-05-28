CREATE TABLE [dbo].[s_mdm_golden_record_customer_email] (
    [s_mdm_golden_record_customer_email_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)      NOT NULL,
    [email_type]                            CHAR (40)      NULL,
    [email]                                 CHAR (128)     NULL,
    [load_date_time]                        DATETIME2 (7)  NULL,
    [entity_id]                             NVARCHAR (128) NULL,
    [inserted_date_time]                    DATETIME       NULL,
    [dv_load_date_time]                     DATETIME       NOT NULL,
    [dv_r_load_source_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]                 DATETIME       NOT NULL,
    [dv_insert_user]                        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                  DATETIME       NULL,
    [dv_update_user]                        VARCHAR (50)   NULL,
    [dv_hash]                               CHAR (32)      NOT NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mdm_golden_record_customer_email]([dv_batch_id] ASC);

