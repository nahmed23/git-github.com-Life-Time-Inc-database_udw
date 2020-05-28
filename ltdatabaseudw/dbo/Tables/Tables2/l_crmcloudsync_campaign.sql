CREATE TABLE [dbo].[l_crmcloudsync_campaign] (
    [l_crmcloudsync_campaign_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [campaign_id]                VARCHAR (36)   NULL,
    [created_by]                 VARCHAR (36)   NULL,
    [modified_by]                VARCHAR (36)   NULL,
    [owner_id]                   VARCHAR (36)   NULL,
    [owning_business_unit]       VARCHAR (36)   NULL,
    [owning_user]                VARCHAR (36)   NULL,
    [transaction_currency_id]    VARCHAR (36)   NULL,
    [ltf_job_id]                 NVARCHAR (100) NULL,
    [dv_load_date_time]          DATETIME       NOT NULL,
    [dv_r_load_source_id]        BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL,
    [dv_hash]                    CHAR (32)      NOT NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_campaign]([dv_batch_id] ASC);

