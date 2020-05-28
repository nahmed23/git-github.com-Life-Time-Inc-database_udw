CREATE TABLE [dbo].[s_ig_ig_dimension_business_period_dimension] (
    [s_ig_ig_dimension_business_period_dimension_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)    NOT NULL,
    [business_period_dim_id]                         BIGINT       NULL,
    [start_date_time]                                DATETIME     NULL,
    [end_date_time]                                  DATETIME     NULL,
    [dv_load_date_time]                              DATETIME     NOT NULL,
    [dv_batch_id]                                    BIGINT       NOT NULL,
    [dv_r_load_source_id]                            BIGINT       NOT NULL,
    [dv_inserted_date_time]                          DATETIME     NOT NULL,
    [dv_insert_user]                                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                           DATETIME     NULL,
    [dv_update_user]                                 VARCHAR (50) NULL,
    [dv_hash]                                        CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_ig_dimension_business_period_dimension]
    ON [dbo].[s_ig_ig_dimension_business_period_dimension]([bk_hash] ASC, [s_ig_ig_dimension_business_period_dimension_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_ig_dimension_business_period_dimension]([dv_batch_id] ASC);

