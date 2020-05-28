CREATE TABLE [dbo].[l_crmcloudsync_ltf_program_cycle] (
    [l_crmcloudsync_ltf_program_cycle_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [created_by]                          VARCHAR (36) NULL,
    [created_on_behalf_by]                VARCHAR (36) NULL,
    [import_sequence_number]              INT          NULL,
    [ltf_program]                         INT          NULL,
    [ltf_program_cycle_id]                VARCHAR (36) NULL,
    [modified_by]                         VARCHAR (36) NULL,
    [modified_on_behalf_by]               VARCHAR (36) NULL,
    [owner_id]                            VARCHAR (36) NULL,
    [owning_business_unit]                VARCHAR (36) NULL,
    [owning_team]                         VARCHAR (36) NULL,
    [owning_user]                         VARCHAR (36) NULL,
    [state_code]                          INT          NULL,
    [status_code]                         INT          NULL,
    [version_number]                      BIGINT       NULL,
    [dv_load_date_time]                   DATETIME     NOT NULL,
    [dv_r_load_source_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL,
    [dv_hash]                             CHAR (32)    NOT NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_ltf_program_cycle]
    ON [dbo].[l_crmcloudsync_ltf_program_cycle]([bk_hash] ASC, [l_crmcloudsync_ltf_program_cycle_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_program_cycle]([dv_batch_id] ASC);

