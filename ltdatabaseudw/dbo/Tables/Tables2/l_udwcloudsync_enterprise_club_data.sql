﻿CREATE TABLE [dbo].[l_udwcloudsync_enterprise_club_data] (
    [l_udwcloudsync_enterprise_club_data_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)       NOT NULL,
    [enterprise_club_data_id]                INT             NULL,
    [mms_club_id]                            NVARCHAR (4000) NULL,
    [dv_load_date_time]                      DATETIME        NOT NULL,
    [dv_r_load_source_id]                    BIGINT          NOT NULL,
    [dv_inserted_date_time]                  DATETIME        NOT NULL,
    [dv_insert_user]                         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                   DATETIME        NULL,
    [dv_update_user]                         VARCHAR (50)    NULL,
    [dv_hash]                                CHAR (32)       NOT NULL,
    [dv_deleted]                             BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

