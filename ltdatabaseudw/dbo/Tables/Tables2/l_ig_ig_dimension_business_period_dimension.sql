CREATE TABLE [dbo].[l_ig_ig_dimension_business_period_dimension] (
    [l_ig_ig_dimension_business_period_dimension_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)    NOT NULL,
    [business_period_dim_id]                         BIGINT       NULL,
    [customer_id]                                    INT          NULL,
    [ent_id]                                         INT          NULL,
    [business_period_group_id]                       INT          NULL,
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
CREATE CLUSTERED INDEX [ci_l_ig_ig_dimension_business_period_dimension]
    ON [dbo].[l_ig_ig_dimension_business_period_dimension]([bk_hash] ASC, [l_ig_ig_dimension_business_period_dimension_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_ig_dimension_business_period_dimension]([dv_batch_id] ASC);

