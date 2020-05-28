﻿CREATE TABLE [dbo].[p_commprefs_communication_type_channel_membership_segments] (
    [p_commprefs_communication_type_channel_membership_segments_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                       CHAR (32)    NOT NULL,
    [communication_type_channel_membership_segments_id]             INT          NULL,
    [l_commprefs_communication_type_channel_membership_segments_id] BIGINT       NULL,
    [s_commprefs_communication_type_channel_membership_segments_id] BIGINT       NULL,
    [dv_load_date_time]                                             DATETIME     NOT NULL,
    [dv_load_end_date_time]                                         DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]                               DATETIME     NULL,
    [dv_next_greatest_satellite_date_time]                          DATETIME     NULL,
    [dv_first_in_key_series]                                        INT          NULL,
    [dv_inserted_date_time]                                         DATETIME     NOT NULL,
    [dv_insert_user]                                                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                                          DATETIME     NULL,
    [dv_update_user]                                                VARCHAR (50) NULL,
    [dv_batch_id]                                                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_commprefs_communication_type_channel_membership_segments]
    ON [dbo].[p_commprefs_communication_type_channel_membership_segments]([bk_hash] ASC, [p_commprefs_communication_type_channel_membership_segments_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_commprefs_communication_type_channel_membership_segments]([dv_batch_id] ASC);

