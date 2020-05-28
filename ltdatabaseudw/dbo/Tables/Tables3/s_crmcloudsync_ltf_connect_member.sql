CREATE TABLE [dbo].[s_crmcloudsync_ltf_connect_member] (
    [s_crmcloudsync_ltf_connect_member_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)      NOT NULL,
    [created_by_name]                      NVARCHAR (200) NULL,
    [created_by_yomi_name]                 NVARCHAR (200) NULL,
    [created_on]                           DATETIME       NULL,
    [created_on_behalf_by_name]            NVARCHAR (200) NULL,
    [created_on_behalf_by_yomi_name]       NVARCHAR (200) NULL,
    [import_sequence_number]               INT            NULL,
    [ltf_connect_member_id]                VARCHAR (36)   NULL,
    [ltf_connect_notes]                    VARCHAR (8000) NULL,
    [ltf_link_description]                 NVARCHAR (150) NULL,
    [ltf_move_it_scheduled_date]           DATETIME       NULL,
    [ltf_move_it_scheduled_with]           NVARCHAR (100) NULL,
    [ltf_opportunity_id_name]              NVARCHAR (300) NULL,
    [ltf_profile_notes]                    VARCHAR (8000) NULL,
    [ltf_programs_of_interest]             INT            NULL,
    [ltf_programs_of_interest_name]        NVARCHAR (255) NULL,
    [ltf_subscriber_id_name]               NVARCHAR (100) NULL,
    [ltf_want_to_do]                       INT            NULL,
    [ltf_want_to_do_name]                  NVARCHAR (255) NULL,
    [ltf_who_met_with]                     NVARCHAR (100) NULL,
    [ltf_why_want_to_do]                   INT            NULL,
    [ltf_why_want_to_do_name]              NVARCHAR (255) NULL,
    [modified_by_name]                     NVARCHAR (200) NULL,
    [modified_by_yomi_name]                NVARCHAR (200) NULL,
    [modified_on]                          DATETIME       NULL,
    [modified_on_behalf_by_name]           NVARCHAR (200) NULL,
    [modified_on_behalf_by_yomi_name]      NVARCHAR (200) NULL,
    [overridden_created_on]                DATETIME       NULL,
    [owner_id_name]                        NVARCHAR (200) NULL,
    [owner_id_type]                        NVARCHAR (64)  NULL,
    [owner_id_yomi_name]                   NVARCHAR (200) NULL,
    [state_code]                           INT            NULL,
    [state_code_name]                      NVARCHAR (255) NULL,
    [status_code]                          INT            NULL,
    [status_code_name]                     NVARCHAR (255) NULL,
    [time_zone_rule_version_number]        INT            NULL,
    [utc_conversion_time_zone_code]        INT            NULL,
    [version_number]                       BIGINT         NULL,
    [inserted_date_time]                   DATETIME       NULL,
    [insert_user]                          VARCHAR (100)  NULL,
    [updated_date_time]                    DATETIME       NULL,
    [update_user]                          VARCHAR (50)   NULL,
    [dv_load_date_time]                    DATETIME       NOT NULL,
    [dv_batch_id]                          BIGINT         NOT NULL,
    [dv_r_load_source_id]                  BIGINT         NOT NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL,
    [dv_hash]                              CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_crmcloudsync_ltf_connect_member]
    ON [dbo].[s_crmcloudsync_ltf_connect_member]([bk_hash] ASC, [s_crmcloudsync_ltf_connect_member_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_ltf_connect_member]([dv_batch_id] ASC);

