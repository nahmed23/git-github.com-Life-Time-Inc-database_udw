﻿CREATE TABLE [dbo].[l_fitmetrix_api_appointment_id_statistics] (
    [l_fitmetrix_api_appointment_id_statistics_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)     NOT NULL,
    [profile_appointment_id]                       INT           NULL,
    [profile_id]                                   INT           NULL,
    [external_id]                                  VARCHAR (255) NULL,
    [appointment_id]                               INT           NULL,
    [device_id]                                    INT           NULL,
    [loaner_device_id]                             INT           NULL,
    [spot_device_id]                               VARCHAR (255) NULL,
    [dv_load_date_time]                            DATETIME      NOT NULL,
    [dv_r_load_source_id]                          BIGINT        NOT NULL,
    [dv_inserted_date_time]                        DATETIME      NOT NULL,
    [dv_insert_user]                               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                         DATETIME      NULL,
    [dv_update_user]                               VARCHAR (50)  NULL,
    [dv_hash]                                      CHAR (32)     NOT NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_fitmetrix_api_appointment_id_statistics]([dv_batch_id] ASC);

