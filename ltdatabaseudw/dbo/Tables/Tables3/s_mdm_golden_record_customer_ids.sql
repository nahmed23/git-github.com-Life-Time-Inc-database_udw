CREATE TABLE [dbo].[s_mdm_golden_record_customer_ids] (
    [s_mdm_golden_record_customer_ids_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [load_date_time]                      DATETIME2 (7)  NULL,
    [entity_id]                           NVARCHAR (128) NULL,
    [source_code]                         VARCHAR (128)  NULL,
    [source_id]                           VARCHAR (128)  NULL,
    [inserted_date_time]                  DATETIME2 (7)  NULL,
    [dv_load_date_time]                   DATETIME       NOT NULL,
    [dv_r_load_source_id]                 BIGINT         NOT NULL,
    [dv_inserted_date_time]               DATETIME       NOT NULL,
    [dv_insert_user]                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL,
    [dv_hash]                             CHAR (32)      NOT NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mdm_golden_record_customer_ids]([dv_batch_id] ASC);

