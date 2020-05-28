CREATE TABLE [dbo].[s_boss_asi_invtr] (
    [s_boss_asi_invtr_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [invtr_upc_code]        CHAR (15)       NULL,
    [invtr_desc]            CHAR (50)       NULL,
    [invtr_color]           CHAR (8)        NULL,
    [invtr_style]           CHAR (8)        NULL,
    [invtr_price]           DECIMAL (10, 3) NULL,
    [invtr_cost]            DECIMAL (10, 2) NULL,
    [invtr_promo_part]      CHAR (1)        NULL,
    [invtr_suggestion]      CHAR (48)       NULL,
    [invtr_active_promo]    INT             NULL,
    [invtr_sku]             CHAR (18)       NULL,
    [invtr_created]         DATETIME        NULL,
    [invtr_last_sold]       DATETIME        NULL,
    [invtr_display]         CHAR (1)        NULL,
    [invtr_target]          INT             NULL,
    [invtr_limit]           INT             NULL,
    [invtr_iskit]           CHAR (1)        NULL,
    [invtr_updated_at]      DATETIME        NULL,
    [use_for_ltbucks]       CHAR (1)        NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_asi_invtr]([dv_batch_id] ASC);

