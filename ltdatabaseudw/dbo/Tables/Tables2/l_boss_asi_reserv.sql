CREATE TABLE [dbo].[l_boss_asi_reserv] (
    [l_boss_asi_reserv_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [reservation]           INT          NULL,
    [trainer_cust_code]     CHAR (10)    NULL,
    [upc_code]              CHAR (15)    NULL,
    [club]                  INT          NULL,
    [resource_id]           INT          NULL,
    [link_to]               INT          NULL,
    [interest_id]           INT          NULL,
    [format_id]             INT          NULL,
    [mms_product_id]        CHAR (15)    NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL,
    [dv_deleted]            BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_boss_asi_reserv]([dv_batch_id] ASC);

