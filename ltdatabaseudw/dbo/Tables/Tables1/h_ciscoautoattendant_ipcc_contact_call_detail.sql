CREATE TABLE [dbo].[h_ciscoautoattendant_ipcc_contact_call_detail] (
    [h_ciscoautoattendant_ipcc_contact_call_detail_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)    NOT NULL,
    [session_id]                                       DECIMAL (18) NULL,
    [session_seq_num]                                  SMALLINT     NULL,
    [node_id]                                          SMALLINT     NULL,
    [profile_id]                                       INT          NULL,
    [dv_load_date_time]                                DATETIME     NOT NULL,
    [dv_r_load_source_id]                              BIGINT       NOT NULL,
    [dv_inserted_date_time]                            DATETIME     NOT NULL,
    [dv_insert_user]                                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                             DATETIME     NULL,
    [dv_update_user]                                   VARCHAR (50) NULL,
    [dv_deleted]                                       BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                                      BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_h_ciscoautoattendant_ipcc_contact_call_detail]
    ON [dbo].[h_ciscoautoattendant_ipcc_contact_call_detail]([bk_hash] ASC, [h_ciscoautoattendant_ipcc_contact_call_detail_id] ASC);

