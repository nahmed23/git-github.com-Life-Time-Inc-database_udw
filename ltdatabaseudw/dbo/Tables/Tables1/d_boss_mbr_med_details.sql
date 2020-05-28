CREATE TABLE [dbo].[d_boss_mbr_med_details] (
    [d_boss_mbr_med_details_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [mbr_med_details_id]        INT            NULL,
    [allergy_info]              VARCHAR (4000) NULL,
    [created_dim_date_key]      CHAR (8)       NULL,
    [created_dim_time_key]      CHAR (8)       NULL,
    [immun_current]             INT            NULL,
    [mbr_med_details_cust_code] VARCHAR (10)   NULL,
    [mbr_med_details_mbr_code]  VARCHAR (10)   NULL,
    [med_admin_auth]            INT            NULL,
    [med_info]                  VARCHAR (4000) NULL,
    [updated_dim_date_key]      CHAR (8)       NULL,
    [updated_dim_time_key]      CHAR (8)       NULL,
    [p_boss_mbr_med_details_id] BIGINT         NOT NULL,
    [deleted_flag]              INT            NULL,
    [dv_load_date_time]         DATETIME       NULL,
    [dv_load_end_date_time]     DATETIME       NULL,
    [dv_batch_id]               BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_mbr_med_details]([dv_batch_id] ASC);

