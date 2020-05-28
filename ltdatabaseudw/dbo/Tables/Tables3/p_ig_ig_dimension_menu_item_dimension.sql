CREATE TABLE [dbo].[p_ig_ig_dimension_menu_item_dimension] (
    [p_ig_ig_dimension_menu_item_dimension_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)    NOT NULL,
    [menu_item_dim_id]                         BIGINT       NULL,
    [l_ig_ig_dimension_menu_item_dimension_id] BIGINT       NULL,
    [s_ig_ig_dimension_menu_item_dimension_id] BIGINT       NULL,
    [dv_load_date_time]                        DATETIME     NOT NULL,
    [dv_load_end_date_time]                    DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]          DATETIME     NULL,
    [dv_next_greatest_satellite_date_time]     DATETIME     NULL,
    [dv_first_in_key_series]                   INT          NULL,
    [dv_inserted_date_time]                    DATETIME     NOT NULL,
    [dv_insert_user]                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                     DATETIME     NULL,
    [dv_update_user]                           VARCHAR (50) NULL,
    [dv_batch_id]                              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_ig_ig_dimension_menu_item_dimension]
    ON [dbo].[p_ig_ig_dimension_menu_item_dimension]([bk_hash] ASC, [p_ig_ig_dimension_menu_item_dimension_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_ig_ig_dimension_menu_item_dimension]([dv_batch_id] ASC);

