CREATE TABLE [dbo].[s_ig_ig_dimension_tender_dimension] (
    [s_ig_ig_dimension_tender_dimension_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)     NOT NULL,
    [tender_dim_id]                         BIGINT        NULL,
    [tender_name]                           NVARCHAR (50) NULL,
    [tender_class_name]                     NVARCHAR (50) NULL,
    [cash_tender_flag]                      BIT           NULL,
    [comp_tender_flag]                      BIT           NULL,
    [eff_date_from]                         DATETIME      NULL,
    [eff_date_to]                           DATETIME      NULL,
    [dv_load_date_time]                     DATETIME      NOT NULL,
    [dv_batch_id]                           BIGINT        NOT NULL,
    [dv_r_load_source_id]                   BIGINT        NOT NULL,
    [dv_inserted_date_time]                 DATETIME      NOT NULL,
    [dv_insert_user]                        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                  DATETIME      NULL,
    [dv_update_user]                        VARCHAR (50)  NULL,
    [dv_hash]                               CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_ig_dimension_tender_dimension]
    ON [dbo].[s_ig_ig_dimension_tender_dimension]([bk_hash] ASC, [s_ig_ig_dimension_tender_dimension_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_ig_dimension_tender_dimension]([dv_batch_id] ASC);

