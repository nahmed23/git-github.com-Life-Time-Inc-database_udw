CREATE TABLE [dbo].[s_boss_mbr_addresses] (
    [s_boss_mbr_addresses_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)    NOT NULL,
    [mbr_addresses_id]        INT          NULL,
    [line_1]                  VARCHAR (40) NULL,
    [line_2]                  VARCHAR (40) NULL,
    [city]                    VARCHAR (40) NULL,
    [zip]                     VARCHAR (5)  NULL,
    [zip_four]                VARCHAR (4)  NULL,
    [state_code]              VARCHAR (2)  NULL,
    [addr_type]               VARCHAR (1)  NULL,
    [created_at]              DATETIME     NULL,
    [updated_at]              DATETIME     NULL,
    [dv_load_date_time]       DATETIME     NOT NULL,
    [dv_r_load_source_id]     BIGINT       NOT NULL,
    [dv_inserted_date_time]   DATETIME     NOT NULL,
    [dv_insert_user]          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]    DATETIME     NULL,
    [dv_update_user]          VARCHAR (50) NULL,
    [dv_hash]                 CHAR (32)    NOT NULL,
    [dv_deleted]              BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_mbr_addresses]([dv_batch_id] ASC);

