CREATE TABLE [dbo].[s_ig_it_trn_tender_terminal_cum_BD_1] (
    [s_ig_it_trn_tender_terminal_cum_BD_1_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)    NOT NULL,
    [bus_day_id]                              INT          NULL,
    [check_type_id]                           INT          NULL,
    [meal_period_id]                          INT          NULL,
    [profit_center_id]                        INT          NULL,
    [tender_id]                               INT          NULL,
    [term_id]                                 INT          NULL,
    [void_type_id]                            INT          NULL,
    [bd_start_dt_time]                        DATETIME     NULL,
    [bd_end_dt_time]                          DATETIME     NULL,
    [dv_load_date_time]                       DATETIME     NOT NULL,
    [dv_r_load_source_id]                     BIGINT       NOT NULL,
    [dv_inserted_date_time]                   DATETIME     NOT NULL,
    [dv_insert_user]                          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                    DATETIME     NULL,
    [dv_update_user]                          VARCHAR (50) NULL,
    [dv_hash]                                 CHAR (32)    NOT NULL,
    [dv_deleted]                              BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

