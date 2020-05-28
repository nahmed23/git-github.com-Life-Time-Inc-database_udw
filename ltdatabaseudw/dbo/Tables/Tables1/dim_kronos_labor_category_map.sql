CREATE TABLE [dbo].[dim_kronos_labor_category_map] (
    [dim_kronos_labor_category_map_id]                BIGINT       IDENTITY (1, 1) NOT NULL,
    [ent_id]                                          INT          NULL,
    [ig_it_cfg_product_class_master_product_class_id] INT          NULL,
    [kronos_labor_category]                           VARCHAR (18) NULL,
    [dv_load_date_time]                               DATETIME     NULL,
    [dv_load_end_date_time]                           DATETIME     NULL,
    [dv_batch_id]                                     BIGINT       NOT NULL,
    [dv_inserted_date_time]                           DATETIME     NOT NULL,
    [dv_insert_user]                                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                            DATETIME     NULL,
    [dv_update_user]                                  VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

