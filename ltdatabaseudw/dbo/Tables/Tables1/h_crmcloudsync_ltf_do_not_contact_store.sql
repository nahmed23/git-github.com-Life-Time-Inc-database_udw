CREATE TABLE [dbo].[h_crmcloudsync_ltf_do_not_contact_store] (
    [h_crmcloudsync_ltf_do_not_contact_store_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [ltf_do_not_contact_store_id]                VARCHAR (36) NULL,
    [dv_load_date_time]                          DATETIME     NOT NULL,
    [dv_r_load_source_id]                        BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL,
    [dv_deleted]                                 BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_h_crmcloudsync_ltf_do_not_contact_store]
    ON [dbo].[h_crmcloudsync_ltf_do_not_contact_store]([bk_hash] ASC, [h_crmcloudsync_ltf_do_not_contact_store_id] ASC);

