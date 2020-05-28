CREATE TABLE [dbo].[d_boss_mbr_phones] (
    [d_boss_mbr_phones_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [mbr_phones_id]               INT          NULL,
    [area_code]                   VARCHAR (3)  NULL,
    [contact_id]                  INT          NULL,
    [created_dim_date_key]        CHAR (8)     NULL,
    [created_dim_time_key]        CHAR (8)     NULL,
    [d_boss_mbr_contacts_bk_hash] CHAR (32)    NULL,
    [ext]                         VARCHAR (5)  NULL,
    [number]                      VARCHAR (7)  NULL,
    [ph_type]                     VARCHAR (1)  NULL,
    [updated_dim_date_key]        CHAR (8)     NULL,
    [updated_dim_time_key]        CHAR (8)     NULL,
    [p_boss_mbr_phones_id]        BIGINT       NOT NULL,
    [deleted_flag]                INT          NULL,
    [dv_load_date_time]           DATETIME     NULL,
    [dv_load_end_date_time]       DATETIME     NULL,
    [dv_batch_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_mbr_phones]([dv_batch_id] ASC);

