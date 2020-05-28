CREATE TABLE [dbo].[d_boss_mbr_authorized_pickups] (
    [d_boss_mbr_authorized_pickups_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)     NOT NULL,
    [mbr_authorized_pickups_id]        INT           NULL,
    [created_dim_date_key]             CHAR (8)      NULL,
    [created_dim_time_key]             CHAR (8)      NULL,
    [mbr_authorized_pickups_cust_code] VARCHAR (10)  NULL,
    [mbr_authorized_pickups_mbr_code]  VARCHAR (10)  NULL,
    [mbr_authorized_pickups_notes]     VARCHAR (255) NULL,
    [updated_dim_date_key]             CHAR (8)      NULL,
    [updated_dim_time_key]             CHAR (8)      NULL,
    [p_boss_mbr_authorized_pickups_id] BIGINT        NOT NULL,
    [deleted_flag]                     INT           NULL,
    [dv_load_date_time]                DATETIME      NULL,
    [dv_load_end_date_time]            DATETIME      NULL,
    [dv_batch_id]                      BIGINT        NOT NULL,
    [dv_inserted_date_time]            DATETIME      NOT NULL,
    [dv_insert_user]                   VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]             DATETIME      NULL,
    [dv_update_user]                   VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_mbr_authorized_pickups]([dv_batch_id] ASC);

