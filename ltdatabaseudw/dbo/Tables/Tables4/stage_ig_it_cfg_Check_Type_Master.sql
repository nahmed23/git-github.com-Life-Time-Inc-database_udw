CREATE TABLE [dbo].[stage_ig_it_cfg_Check_Type_Master] (
    [stage_ig_it_cfg_Check_Type_Master_id] BIGINT        NOT NULL,
    [ent_id]                               INT           NULL,
    [check_type_id]                        INT           NULL,
    [check_type_name]                      NVARCHAR (16) NULL,
    [check_type_abbr1]                     NVARCHAR (7)  NULL,
    [check_type_abbr2]                     NVARCHAR (7)  NULL,
    [default_sec_id]                       INT           NULL,
    [default_price_level_id]               INT           NULL,
    [sales_tippable_flag]                  BIT           NULL,
    [store_id]                             INT           NULL,
    [round_basis]                          INT           NULL,
    [round_type_id]                        SMALLINT      NULL,
    [row_version]                          BINARY (8)    NULL,
    [discount_id]                          INT           NULL,
    [dummy_modified_date_time]             DATETIME      NULL,
    [dv_batch_id]                          BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

