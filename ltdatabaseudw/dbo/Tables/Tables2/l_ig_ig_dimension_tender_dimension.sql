CREATE TABLE [dbo].[l_ig_ig_dimension_tender_dimension] (
    [l_ig_ig_dimension_tender_dimension_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [tender_dim_id]                         BIGINT       NULL,
    [profit_center_dim_level_2_id]          INT          NULL,
    [tender_id]                             INT          NULL,
    [tender_class_id]                       INT          NULL,
    [customer_id]                           INT          NULL,
    [ent_id]                                INT          NULL,
    [corp_id]                               INT          NULL,
    [dv_load_date_time]                     DATETIME     NOT NULL,
    [dv_batch_id]                           BIGINT       NOT NULL,
    [dv_r_load_source_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL,
    [dv_hash]                               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ig_ig_dimension_tender_dimension]
    ON [dbo].[l_ig_ig_dimension_tender_dimension]([bk_hash] ASC, [l_ig_ig_dimension_tender_dimension_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_ig_dimension_tender_dimension]([dv_batch_id] ASC);

