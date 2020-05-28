CREATE TABLE [dbo].[s_boss_mbr_med_details] (
    [s_boss_mbr_med_details_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [mbr_med_details_id]        INT            NULL,
    [cust_code]                 VARCHAR (10)   NULL,
    [mbr_code]                  VARCHAR (10)   NULL,
    [med_admin_auth]            INT            NULL,
    [immun_current]             INT            NULL,
    [med_info]                  VARCHAR (4000) NULL,
    [allergy_info]              VARCHAR (4000) NULL,
    [created_at]                DATETIME       NULL,
    [updated_at]                DATETIME       NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL,
    [dv_deleted]                BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_mbr_med_details]([dv_batch_id] ASC);

