CREATE TABLE [dbo].[p_crmcloudsync_contact] (
    [p_crmcloudsync_contact_id]            BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [contact_id]                           VARCHAR (36) NULL,
    [l_crmcloudsync_contact_id]            BIGINT       NULL,
    [s_crmcloudsync_contact_id]            BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_crmcloudsync_contact]
    ON [dbo].[p_crmcloudsync_contact]([bk_hash] ASC, [p_crmcloudsync_contact_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_crmcloudsync_contact]([dv_batch_id] ASC);

