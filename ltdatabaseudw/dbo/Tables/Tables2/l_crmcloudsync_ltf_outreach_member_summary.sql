CREATE TABLE [dbo].[l_crmcloudsync_ltf_outreach_member_summary] (
    [l_crmcloudsync_ltf_outreach_member_summary_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)    NOT NULL,
    [created_by]                                    VARCHAR (36) NULL,
    [created_on_behalf_by]                          VARCHAR (36) NULL,
    [ltf_claimed_by]                                VARCHAR (36) NULL,
    [ltf_club_id]                                   VARCHAR (36) NULL,
    [ltf_contact]                                   VARCHAR (36) NULL,
    [ltf_enrolled_by]                               VARCHAR (36) NULL,
    [ltf_initial_contact_by_cycle]                  VARCHAR (36) NULL,
    [ltf_last_attempt_by]                           VARCHAR (36) NULL,
    [ltf_last_contact_by]                           VARCHAR (36) NULL,
    [ltf_outreach_member_summary_id]                VARCHAR (36) NULL,
    [ltf_product]                                   VARCHAR (36) NULL,
    [ltf_program_cycle_reference]                   VARCHAR (36) NULL,
    [ltf_subscription_id]                           VARCHAR (36) NULL,
    [modified_by]                                   VARCHAR (36) NULL,
    [modified_on_behalf_by]                         VARCHAR (36) NULL,
    [owner_id]                                      VARCHAR (36) NULL,
    [owning_business_unit]                          VARCHAR (36) NULL,
    [owning_team]                                   VARCHAR (36) NULL,
    [owning_user]                                   VARCHAR (36) NULL,
    [dv_load_date_time]                             DATETIME     NOT NULL,
    [dv_r_load_source_id]                           BIGINT       NOT NULL,
    [dv_inserted_date_time]                         DATETIME     NOT NULL,
    [dv_insert_user]                                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                          DATETIME     NULL,
    [dv_update_user]                                VARCHAR (50) NULL,
    [dv_hash]                                       CHAR (32)    NOT NULL,
    [dv_batch_id]                                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_ltf_outreach_member_summary]
    ON [dbo].[l_crmcloudsync_ltf_outreach_member_summary]([bk_hash] ASC, [l_crmcloudsync_ltf_outreach_member_summary_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_outreach_member_summary]([dv_batch_id] ASC);

