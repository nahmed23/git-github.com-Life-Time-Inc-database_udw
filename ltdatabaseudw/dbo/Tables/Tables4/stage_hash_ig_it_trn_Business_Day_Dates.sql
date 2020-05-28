CREATE TABLE [dbo].[stage_hash_ig_it_trn_Business_Day_Dates] (
    [stage_hash_ig_it_trn_Business_Day_Dates_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [BD_end_dttime]                              DATETIME     NULL,
    [BD_start_dttime]                            DATETIME     NULL,
    [bus_day_id]                                 INT          NULL,
    [dv_load_date_time]                          DATETIME     NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL,
    [dv_batch_id]                                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

