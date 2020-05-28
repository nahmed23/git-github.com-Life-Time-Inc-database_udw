﻿CREATE TABLE [dbo].[h_exacttarget_send_job_impression] (
    [h_exacttarget_send_job_impression_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)    NOT NULL,
    [stage_exacttarget_send_job_impression_id] BIGINT       NULL,
    [client_id]                                BIGINT       NULL,
    [send_id]                                  BIGINT       NULL,
    [impression_region_id]                     BIGINT       NULL,
    [dv_load_date_time]                        DATETIME     NOT NULL,
    [dv_batch_id]                              BIGINT       NOT NULL,
    [dv_r_load_source_id]                      BIGINT       NOT NULL,
    [dv_inserted_date_time]                    DATETIME     NOT NULL,
    [dv_insert_user]                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                     DATETIME     NULL,
    [dv_update_user]                           VARCHAR (50) NULL,
    [dv_deleted]                               BIT          DEFAULT ((0)) NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_h_exacttarget_send_job_impression]
    ON [dbo].[h_exacttarget_send_job_impression]([bk_hash] ASC, [h_exacttarget_send_job_impression_id] ASC);

