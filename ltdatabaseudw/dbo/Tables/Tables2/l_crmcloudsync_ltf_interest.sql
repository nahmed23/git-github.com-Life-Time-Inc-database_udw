CREATE TABLE [dbo].[l_crmcloudsync_ltf_interest] (
    [l_crmcloudsync_ltf_interest_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)     NOT NULL,
    [created_by]                     VARCHAR (36)  NULL,
    [created_on_behalf_by]           VARCHAR (36)  NULL,
    [ltf_interest_id]                VARCHAR (36)  NULL,
    [ltf_mms_id]                     NVARCHAR (10) NULL,
    [modified_by]                    VARCHAR (36)  NULL,
    [modified_on_behalf_by]          VARCHAR (36)  NULL,
    [organization_id]                VARCHAR (36)  NULL,
    [state_code]                     INT           NULL,
    [status_code]                    INT           NULL,
    [version_number]                 BIGINT        NULL,
    [dv_load_date_time]              DATETIME      NOT NULL,
    [dv_r_load_source_id]            BIGINT        NOT NULL,
    [dv_inserted_date_time]          DATETIME      NOT NULL,
    [dv_insert_user]                 VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]           DATETIME      NULL,
    [dv_update_user]                 VARCHAR (50)  NULL,
    [dv_hash]                        CHAR (32)     NOT NULL,
    [dv_batch_id]                    BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_interest]([dv_batch_id] ASC);

