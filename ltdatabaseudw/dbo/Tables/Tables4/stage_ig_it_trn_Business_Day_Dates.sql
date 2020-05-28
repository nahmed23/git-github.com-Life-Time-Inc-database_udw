CREATE TABLE [dbo].[stage_ig_it_trn_Business_Day_Dates] (
    [stage_ig_it_trn_Business_Day_Dates_id] BIGINT   NOT NULL,
    [BD_end_dttime]                         DATETIME NULL,
    [BD_start_dttime]                       DATETIME NULL,
    [bus_day_id]                            INT      NULL,
    [dv_batch_id]                           BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

