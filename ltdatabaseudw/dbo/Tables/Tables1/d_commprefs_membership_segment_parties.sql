CREATE TABLE [dbo].[d_commprefs_membership_segment_parties] (
    [d_commprefs_membership_segment_parties_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [d_commprefs_membership_segment_parties_key] CHAR (32)    NULL,
    [membership_segment_id]                      INT          NULL,
    [party_id]                                   INT          NULL,
    [d_commprefs_membership_segments_bk_hash]    CHAR (32)    NULL,
    [d_commprefs_parties_bk_hash]                CHAR (32)    NULL,
    [p_commprefs_membership_segment_parties_id]  BIGINT       NOT NULL,
    [dv_load_date_time]                          DATETIME     NULL,
    [dv_load_end_date_time]                      DATETIME     NULL,
    [dv_batch_id]                                BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_commprefs_membership_segment_parties]([dv_batch_id] ASC);

