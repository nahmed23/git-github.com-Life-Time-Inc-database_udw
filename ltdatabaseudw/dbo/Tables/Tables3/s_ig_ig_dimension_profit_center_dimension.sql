CREATE TABLE [dbo].[s_ig_ig_dimension_profit_center_dimension] (
    [s_ig_ig_dimension_profit_center_dimension_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)     NOT NULL,
    [profit_center_dim_id]                         INT           NULL,
    [customer_name]                                NVARCHAR (50) NULL,
    [ent_name]                                     NVARCHAR (50) NULL,
    [store_name]                                   NVARCHAR (50) NULL,
    [profit_center_name]                           NVARCHAR (50) NULL,
    [store_tax_no]                                 NVARCHAR (50) NULL,
    [time_zone]                                    NVARCHAR (50) NULL,
    [store_address]                                NVARCHAR (50) NULL,
    [store_zip]                                    NVARCHAR (50) NULL,
    [store_city]                                   NVARCHAR (50) NULL,
    [store_state]                                  NVARCHAR (50) NULL,
    [store_country]                                NVARCHAR (50) NULL,
    [store_size]                                   NVARCHAR (50) NULL,
    [hierarchy_level]                              SMALLINT      NULL,
    [hierarchy_name]                               NVARCHAR (50) NULL,
    [eff_date_from]                                DATETIME      NULL,
    [eff_date_to]                                  DATETIME      NULL,
    [profit_center_desc]                           NVARCHAR (50) NULL,
    [dv_load_date_time]                            DATETIME      NOT NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL,
    [dv_r_load_source_id]                          BIGINT        NOT NULL,
    [dv_inserted_date_time]                        DATETIME      NOT NULL,
    [dv_insert_user]                               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                         DATETIME      NULL,
    [dv_update_user]                               VARCHAR (50)  NULL,
    [dv_hash]                                      CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_ig_dimension_profit_center_dimension]
    ON [dbo].[s_ig_ig_dimension_profit_center_dimension]([bk_hash] ASC, [s_ig_ig_dimension_profit_center_dimension_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_ig_dimension_profit_center_dimension]([dv_batch_id] ASC);

