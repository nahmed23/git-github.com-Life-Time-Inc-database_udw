CREATE TABLE [dbo].[stage_hash_chronotrack_event_location] (
    [stage_hash_chronotrack_event_location_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32) NOT NULL,
    [id]                                       INT       NULL,
    [event_id]                                 BIGINT    NULL,
    [location_id]                              BIGINT    NULL,
    [ctime]                                    INT       NULL,
    [mtime]                                    INT       NULL,
    [dummy_modified_date_time]                 DATETIME  NULL,
    [dv_load_date_time]                        DATETIME  NOT NULL,
    [dv_batch_id]                              BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

