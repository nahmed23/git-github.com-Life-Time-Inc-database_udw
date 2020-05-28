CREATE TABLE [dbo].[dim_goal_line_item] (
    [dim_goal_line_item_id]   BIGINT       IDENTITY (1, 1) NOT NULL,
    [category_description]    VARCHAR (50) NULL,
    [description]             VARCHAR (50) NULL,
    [dim_goal_line_item_key]  VARCHAR (32) NULL,
    [dollar_amount_flag]      CHAR (1)     NULL,
    [percentage_flag]         CHAR (1)     NULL,
    [quantity_flag]           CHAR (1)     NULL,
    [quota_flag]              CHAR (1)     NULL,
    [region_type]             VARCHAR (50) NULL,
    [sort_order]              INT          NULL,
    [subcategory_description] VARCHAR (50) NULL,
    [dv_load_date_time]       DATETIME     NULL,
    [dv_load_end_date_time]   DATETIME     NULL,
    [dv_batch_id]             BIGINT       NOT NULL,
    [dv_inserted_date_time]   DATETIME     NOT NULL,
    [dv_insert_user]          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]    DATETIME     NULL,
    [dv_update_user]          VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_goal_line_item_key]));

