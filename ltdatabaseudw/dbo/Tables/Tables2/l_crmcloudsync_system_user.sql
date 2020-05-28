CREATE TABLE [dbo].[l_crmcloudsync_system_user] (
    [l_crmcloudsync_system_user_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [access_mode_name]              NVARCHAR (255) NULL,
    [address_1_address_id]          VARCHAR (36)   NULL,
    [address_2_address_id]          VARCHAR (36)   NULL,
    [business_unit_id]              VARCHAR (36)   NULL,
    [calendar_id]                   VARCHAR (36)   NULL,
    [created_by]                    VARCHAR (36)   NULL,
    [created_on_behalf_by]          VARCHAR (36)   NULL,
    [default_mail_box]              VARCHAR (36)   NULL,
    [employee_id]                   NVARCHAR (100) NULL,
    [entity_image_id]               VARCHAR (36)   NULL,
    [government_id]                 NVARCHAR (100) NULL,
    [ltf_club_id]                   VARCHAR (36)   NULL,
    [modified_by]                   VARCHAR (36)   NULL,
    [modified_on_behalf_by]         VARCHAR (36)   NULL,
    [organization_id]               VARCHAR (36)   NULL,
    [parent_system_user_id]         VARCHAR (36)   NULL,
    [process_id]                    VARCHAR (36)   NULL,
    [queue_id]                      VARCHAR (36)   NULL,
    [site_id]                       VARCHAR (36)   NULL,
    [stage_id]                      VARCHAR (36)   NULL,
    [system_user_id]                VARCHAR (36)   NULL,
    [territory_id]                  VARCHAR (36)   NULL,
    [transaction_currency_id]       VARCHAR (36)   NULL,
    [yammer_user_id]                NVARCHAR (128) NULL,
    [mobile_offline_profile_id]     VARCHAR (36)   NULL,
    [position_id]                   VARCHAR (36)   NULL,
    [dv_load_date_time]             DATETIME       NOT NULL,
    [dv_r_load_source_id]           BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL,
    [dv_hash]                       CHAR (32)      NOT NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_system_user]
    ON [dbo].[l_crmcloudsync_system_user]([bk_hash] ASC, [l_crmcloudsync_system_user_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_system_user]([dv_batch_id] ASC);

