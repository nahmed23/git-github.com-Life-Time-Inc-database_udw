CREATE TABLE [dbo].[d_budget_line_item] (
    [d_budget_line_item_id]    BIGINT        NOT NULL,
    [dim_budget_line_item_key] CHAR (32)     NULL,
    [budget_line_item_id]      BIGINT        NULL,
    [description]              VARCHAR (150) NULL,
    [sub_category_description] VARCHAR (150) NULL,
    [category_description]     VARCHAR (150) NULL,
    [quantity_flag]            CHAR (1)      NOT NULL,
    [dollar_amount_flag]       CHAR (1)      NOT NULL,
    [dv_load_date_time]        DATETIME      NULL,
    [dv_load_end_date_time]    DATETIME      NULL,
    [dv_batch_id]              BIGINT        NULL,
    [dv_inserted_date_time]    DATETIME      NOT NULL,
    [dv_insert_user]           VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]     DATETIME      NULL,
    [dv_update_user]           VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_budget_line_item_key]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_budget_line_item]([dv_batch_id] ASC);

