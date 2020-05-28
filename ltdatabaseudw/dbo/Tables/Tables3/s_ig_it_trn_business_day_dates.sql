CREATE TABLE [dbo].[s_ig_it_trn_business_day_dates] (
    [s_ig_it_trn_business_day_dates_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [bd_end_dttime]                     DATETIME     NULL,
    [bd_start_dttime]                   DATETIME     NULL,
    [bus_day_id]                        INT          NULL,
    [dv_load_date_time]                 DATETIME     NOT NULL,
    [dv_batch_id]                       BIGINT       NOT NULL,
    [dv_r_load_source_id]               BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL,
    [dv_hash]                           CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_it_trn_business_day_dates]
    ON [dbo].[s_ig_it_trn_business_day_dates]([bk_hash] ASC, [s_ig_it_trn_business_day_dates_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_trn_business_day_dates]([dv_batch_id] ASC);

