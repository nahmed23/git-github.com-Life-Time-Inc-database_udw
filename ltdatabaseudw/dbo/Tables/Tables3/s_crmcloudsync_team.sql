CREATE TABLE [dbo].[s_crmcloudsync_team] (
    [s_crmcloudsync_team_id]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [administrator_id_name]           NVARCHAR (200)  NULL,
    [administrator_id_yomi_name]      NVARCHAR (200)  NULL,
    [business_unit_id_name]           NVARCHAR (160)  NULL,
    [created_by_name]                 NVARCHAR (200)  NULL,
    [created_by_yomi_name]            NVARCHAR (200)  NULL,
    [created_on]                      DATETIME        NULL,
    [created_on_behalf_by_name]       NVARCHAR (200)  NULL,
    [created_on_behalf_by_yomi_name]  NVARCHAR (200)  NULL,
    [description]                     NVARCHAR (4000) NULL,
    [email_address]                   NVARCHAR (100)  NULL,
    [exchange_rate]                   DECIMAL (28)    NULL,
    [import_sequence_number]          INT             NULL,
    [is_default]                      BIT             NULL,
    [is_default_name]                 NVARCHAR (255)  NULL,
    [ltf_telephone_1]                 NVARCHAR (15)   NULL,
    [modified_by_name]                NVARCHAR (200)  NULL,
    [modified_by_yomi_name]           NVARCHAR (200)  NULL,
    [modified_on]                     DATETIME        NULL,
    [modified_on_behalf_by_name]      NVARCHAR (200)  NULL,
    [modified_on_behalf_by_yomi_name] NVARCHAR (200)  NULL,
    [name]                            NVARCHAR (160)  NULL,
    [organization_id_name]            NVARCHAR (100)  NULL,
    [overridden_created_on]           DATETIME        NULL,
    [queue_id_name]                   NVARCHAR (400)  NULL,
    [regarding_object_type_code]      NVARCHAR (64)   NULL,
    [system_managed]                  BIT             NULL,
    [system_managed_name]             NVARCHAR (255)  NULL,
    [team_id]                         VARCHAR (36)    NULL,
    [team_type]                       INT             NULL,
    [team_type_name]                  NVARCHAR (255)  NULL,
    [transaction_currency_id_name]    NVARCHAR (100)  NULL,
    [version_number]                  BIGINT          NULL,
    [yomi_name]                       NVARCHAR (160)  NULL,
    [inserted_date_time]              DATETIME        NULL,
    [insert_user]                     VARCHAR (100)   NULL,
    [updated_date_time]               DATETIME        NULL,
    [update_user]                     VARCHAR (50)    NULL,
    [ltf_team_type]                   INT             NULL,
    [ltf_team_type_name]              NVARCHAR (255)  NULL,
    [traversed_path]                  NVARCHAR (1250) NULL,
    [dv_load_date_time]               DATETIME        NOT NULL,
    [dv_batch_id]                     BIGINT          NOT NULL,
    [dv_r_load_source_id]             BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL,
    [dv_hash]                         CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_crmcloudsync_team]
    ON [dbo].[s_crmcloudsync_team]([bk_hash] ASC, [s_crmcloudsync_team_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_team]([dv_batch_id] ASC);

