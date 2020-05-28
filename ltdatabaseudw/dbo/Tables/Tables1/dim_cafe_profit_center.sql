CREATE TABLE [dbo].[dim_cafe_profit_center] (
    [dim_cafe_profit_center_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_cafe_profit_center_key] CHAR (32)     NULL,
    [auto_reconcile_tips_flag]   CHAR (1)      NULL,
    [bistro_flag]                CHAR (1)      NULL,
    [cafe_flag]                  CHAR (1)      NULL,
    [profit_center_id]           INT           NULL,
    [profit_center_name]         NVARCHAR (50) NULL,
    [store_id]                   INT           NULL,
    [store_name]                 NVARCHAR (50) NULL,
    [dv_load_date_time]          DATETIME      NULL,
    [dv_load_end_date_time]      DATETIME      NULL,
    [dv_batch_id]                BIGINT        NULL,
    [dv_inserted_date_time]      DATETIME      NOT NULL,
    [dv_insert_user]             VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]       DATETIME      NULL,
    [dv_update_user]             VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_cafe_profit_center_key]));

