CREATE TABLE [dbo].[d_crmcloudsync_team] (
    [d_crmcloudsync_team_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [dim_crm_team_key]       VARCHAR (32)   NULL,
    [team_id]                VARCHAR (36)   NULL,
    [email_address]          NVARCHAR (100) NULL,
    [ltf_telephone_1]        NVARCHAR (15)  NULL,
    [name]                   NVARCHAR (160) NULL,
    [p_crmcloudsync_team_id] BIGINT         NOT NULL,
    [deleted_flag]           INT            NULL,
    [dv_load_date_time]      DATETIME       NULL,
    [dv_load_end_date_time]  DATETIME       NULL,
    [dv_batch_id]            BIGINT         NOT NULL,
    [dv_inserted_date_time]  DATETIME       NOT NULL,
    [dv_insert_user]         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]   DATETIME       NULL,
    [dv_update_user]         VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

