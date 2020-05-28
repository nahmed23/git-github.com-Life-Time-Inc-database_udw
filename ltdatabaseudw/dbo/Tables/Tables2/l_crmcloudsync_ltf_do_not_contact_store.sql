CREATE TABLE [dbo].[l_crmcloudsync_ltf_do_not_contact_store] (
    [l_crmcloudsync_ltf_do_not_contact_store_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [created_by]                                 VARCHAR (36) NULL,
    [import_sequence_number]                     INT          NULL,
    [ltf_do_not_contact_store_id]                VARCHAR (36) NULL,
    [modifiedby]                                 VARCHAR (36) NULL,
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
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_ltf_do_not_contact_store]
    ON [dbo].[l_crmcloudsync_ltf_do_not_contact_store]([bk_hash] ASC, [l_crmcloudsync_ltf_do_not_contact_store_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_do_not_contact_store]([dv_batch_id] ASC);

