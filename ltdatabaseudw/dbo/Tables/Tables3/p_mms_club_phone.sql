﻿CREATE TABLE [dbo].[p_mms_club_phone] (
    [p_mms_club_phone_id]                  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [club_phone_id]                        INT          NULL,
    [l_mms_club_phone_id]                  BIGINT       NULL,
    [s_mms_club_phone_id]                  BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_mms_club_phone]
    ON [dbo].[p_mms_club_phone]([bk_hash] ASC, [p_mms_club_phone_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_mms_club_phone]([dv_batch_id] ASC);

