CREATE TABLE [dbo].[l_crmcloudsync_contact] (
    [l_crmcloudsync_contact_id]            BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)      NOT NULL,
    [account_id]                           VARCHAR (36)   NULL,
    [address_1_address_id]                 VARCHAR (36)   NULL,
    [address_2_address_id]                 VARCHAR (36)   NULL,
    [contact_id]                           VARCHAR (36)   NULL,
    [created_by]                           VARCHAR (36)   NULL,
    [created_on_behalf_by]                 VARCHAR (36)   NULL,
    [default_price_level_id]               VARCHAR (36)   NULL,
    [employee_id]                          NVARCHAR (50)  NULL,
    [entity_image_id]                      VARCHAR (36)   NULL,
    [government_id]                        NVARCHAR (50)  NULL,
    [ltf_club_id]                          VARCHAR (36)   NULL,
    [ltf_employer_id]                      VARCHAR (36)   NULL,
    [ltf_measurable_goal_id]               VARCHAR (36)   NULL,
    [ltf_primary_objective_id]             VARCHAR (36)   NULL,
    [ltf_referring_contact_id]             VARCHAR (36)   NULL,
    [ltf_referring_member_id]              NVARCHAR (10)  NULL,
    [ltf_specific_goal_id]                 VARCHAR (36)   NULL,
    [ltf_udw_id]                           NVARCHAR (255) NULL,
    [master_id]                            VARCHAR (36)   NULL,
    [modified_by]                          VARCHAR (36)   NULL,
    [modified_on_behalf_by]                VARCHAR (36)   NULL,
    [originating_lead_id]                  VARCHAR (36)   NULL,
    [owner_id]                             VARCHAR (36)   NULL,
    [owning_business_unit]                 VARCHAR (36)   NULL,
    [owning_team]                          VARCHAR (36)   NULL,
    [owning_user]                          VARCHAR (36)   NULL,
    [parent_contact_id]                    VARCHAR (36)   NULL,
    [parent_customer_id]                   VARCHAR (36)   NULL,
    [preferred_equipment_id]               VARCHAR (36)   NULL,
    [preferred_service_id]                 VARCHAR (36)   NULL,
    [preferred_system_user_id]             VARCHAR (36)   NULL,
    [process_id]                           VARCHAR (36)   NULL,
    [stage_id]                             VARCHAR (36)   NULL,
    [transaction_currency_id]              VARCHAR (36)   NULL,
    [ltf_member_id]                        NVARCHAR (10)  NULL,
    [ltf_connect_member_id]                VARCHAR (36)   NULL,
    [created_by_external_party]            VARCHAR (36)   NULL,
    [created_by_external_party_yomi_name]  NVARCHAR (300) NULL,
    [ltf_last_contacted_by]                VARCHAR (36)   NULL,
    [ltf_most_recent_member_id]            NVARCHAR (9)   NULL,
    [modified_by_external_party]           VARCHAR (36)   NULL,
    [modified_by_external_party_yomi_name] NVARCHAR (300) NULL,
    [address_3_address_id]                 VARCHAR (36)   NULL,
    [ltf_ltf_party_id]                     NVARCHAR (10)  NULL,
    [dv_load_date_time]                    DATETIME       NOT NULL,
    [dv_r_load_source_id]                  BIGINT         NOT NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL,
    [dv_hash]                              CHAR (32)      NOT NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_contact]
    ON [dbo].[l_crmcloudsync_contact]([bk_hash] ASC, [l_crmcloudsync_contact_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_contact]([dv_batch_id] ASC);

