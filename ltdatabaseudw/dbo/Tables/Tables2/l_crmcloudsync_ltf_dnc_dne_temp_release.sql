CREATE TABLE [dbo].[l_crmcloudsync_ltf_dnc_dne_temp_release] (
    [l_crmcloudsync_ltf_dnc_dne_temp_release_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [created_by]                                 VARCHAR (36) NULL,
    [created_on_behalf_by]                       VARCHAR (36) NULL,
    [ltf_dnc_dne_temp_release_id]                VARCHAR (36) NULL,
    [modified_by]                                VARCHAR (36) NULL,
    [modified_on_behalf_by]                      VARCHAR (36) NULL,
    [organization_id]                            VARCHAR (36) NULL,
    [dv_load_date_time]                          DATETIME     NOT NULL,
    [dv_r_load_source_id]                        BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL,
    [dv_hash]                                    CHAR (32)    NOT NULL,
    [dv_batch_id]                                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_ltf_dnc_dne_temp_release]
    ON [dbo].[l_crmcloudsync_ltf_dnc_dne_temp_release]([bk_hash] ASC, [l_crmcloudsync_ltf_dnc_dne_temp_release_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_dnc_dne_temp_release]([dv_batch_id] ASC);

