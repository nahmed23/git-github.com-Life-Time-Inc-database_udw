CREATE TABLE [dbo].[stage_chronotrack_event_location] (
    [stage_chronotrack_event_location_id] BIGINT   NOT NULL,
    [id]                                  INT      NULL,
    [event_id]                            BIGINT   NULL,
    [location_id]                         BIGINT   NULL,
    [ctime]                               INT      NULL,
    [mtime]                               INT      NULL,
    [dummy_modified_date_time]            DATETIME NULL,
    [dv_batch_id]                         BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

