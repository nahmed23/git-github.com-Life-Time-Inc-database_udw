CREATE TABLE [dbo].[stage_mart_dim_interest_segment_details] (
    [stage_mart_dim_interest_segment_details_id] BIGINT    NOT NULL,
    [dim_interest_segment_details_id]            INT       NULL,
    [interest_id]                                INT       NULL,
    [interest_name]                              CHAR (50) NULL,
    [row_add_date]                               DATETIME  NULL,
    [active_flag]                                INT       NULL,
    [interest_display_name]                      CHAR (50) NULL,
    [dv_batch_id]                                BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

