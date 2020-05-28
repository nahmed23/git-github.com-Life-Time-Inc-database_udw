CREATE TABLE [dbo].[d_ig_ig_dimension_business_period_dimension] (
    [d_ig_ig_dimension_business_period_dimension_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)    NOT NULL,
    [dim_business_period_dim_key]                    CHAR (32)    NULL,
    [business_period_dim_id]                         BIGINT       NULL,
    [business_period_end_dim_date_key]               CHAR (8)     NULL,
    [business_period_start_dim_date_key]             CHAR (8)     NULL,
    [end_date_time]                                  DATETIME     NULL,
    [month_ending_dim_date_key]                      CHAR (8)     NULL,
    [start_date_time]                                DATETIME     NULL,
    [p_ig_ig_dimension_business_period_dimension_id] BIGINT       NOT NULL,
    [dv_load_date_time]                              DATETIME     NULL,
    [dv_load_end_date_time]                          DATETIME     NULL,
    [dv_batch_id]                                    BIGINT       NOT NULL,
    [dv_inserted_date_time]                          DATETIME     NOT NULL,
    [dv_insert_user]                                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                           DATETIME     NULL,
    [dv_update_user]                                 VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ig_ig_dimension_business_period_dimension]([dv_batch_id] ASC);

