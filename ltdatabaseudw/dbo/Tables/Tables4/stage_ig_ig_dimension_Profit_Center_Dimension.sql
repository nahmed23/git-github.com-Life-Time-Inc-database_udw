CREATE TABLE [dbo].[stage_ig_ig_dimension_Profit_Center_Dimension] (
    [stage_ig_ig_dimension_Profit_Center_Dimension_id] BIGINT        NOT NULL,
    [profit_center_dim_id]                             INT           NULL,
    [customer_id]                                      INT           NULL,
    [ent_id]                                           INT           NULL,
    [store_id]                                         INT           NULL,
    [profit_center_id]                                 INT           NULL,
    [customer_name]                                    NVARCHAR (50) NULL,
    [ent_name]                                         NVARCHAR (50) NULL,
    [store_name]                                       NVARCHAR (50) NULL,
    [profit_center_name]                               NVARCHAR (50) NULL,
    [store_tax_no]                                     NVARCHAR (50) NULL,
    [time_zone]                                        NVARCHAR (50) NULL,
    [store_address]                                    NVARCHAR (50) NULL,
    [store_zip]                                        NVARCHAR (50) NULL,
    [store_city]                                       NVARCHAR (50) NULL,
    [store_state]                                      NVARCHAR (50) NULL,
    [store_country]                                    NVARCHAR (50) NULL,
    [store_type_id]                                    INT           NULL,
    [store_size]                                       NVARCHAR (50) NULL,
    [hierarchy_level]                                  SMALLINT      NULL,
    [hierarchy_name]                                   NVARCHAR (50) NULL,
    [eff_date_from]                                    DATETIME      NULL,
    [eff_date_to]                                      DATETIME      NULL,
    [profit_center_desc]                               NVARCHAR (50) NULL,
    [dv_batch_id]                                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

