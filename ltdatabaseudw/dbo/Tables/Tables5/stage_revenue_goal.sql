CREATE TABLE [dbo].[stage_revenue_goal] (
    [club_id]                          INT             NULL,
    [club_code]                        NVARCHAR (10)   NULL,
    [division]                         NVARCHAR (255)  NULL,
    [sub_division]                     NVARCHAR (255)  NULL,
    [revenue_department]               NVARCHAR (255)  NULL,
    [revenue_product_group_name]       NVARCHAR (255)  NULL,
    [region_type]                      NVARCHAR (255)  NULL,
    [goal_year]                        INT             NULL,
    [quantity_flag]                    CHAR (1)        NULL,
    [dollar_amount_flag]               CHAR (1)        NULL,
    [revenue_product_group_sort_order] INT             NULL,
    [january]                          DECIMAL (26, 6) NULL,
    [february]                         DECIMAL (26, 6) NULL,
    [march]                            DECIMAL (26, 6) NULL,
    [april]                            DECIMAL (26, 6) NULL,
    [may]                              DECIMAL (26, 6) NULL,
    [june]                             DECIMAL (26, 6) NULL,
    [july]                             DECIMAL (26, 6) NULL,
    [august]                           DECIMAL (26, 6) NULL,
    [september]                        DECIMAL (26, 6) NULL,
    [october]                          DECIMAL (26, 6) NULL,
    [november]                         DECIMAL (26, 6) NULL,
    [december]                         DECIMAL (26, 6) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

