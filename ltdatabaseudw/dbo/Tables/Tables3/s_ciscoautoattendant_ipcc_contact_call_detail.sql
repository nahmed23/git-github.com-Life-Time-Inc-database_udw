CREATE TABLE [dbo].[s_ciscoautoattendant_ipcc_contact_call_detail] (
    [s_ciscoautoattendant_ipcc_contact_call_detail_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)     NOT NULL,
    [session_id]                                       DECIMAL (18)  NULL,
    [session_seq_num]                                  SMALLINT      NULL,
    [node_id]                                          SMALLINT      NULL,
    [profile_id]                                       INT           NULL,
    [contact_type]                                     SMALLINT      NULL,
    [contact_disposition]                              SMALLINT      NULL,
    [disposition_reason]                               VARCHAR (100) NULL,
    [originator_type]                                  SMALLINT      NULL,
    [originator_id]                                    INT           NULL,
    [originator_dn]                                    NVARCHAR (30) NULL,
    [destination_type]                                 SMALLINT      NULL,
    [destination_id]                                   INT           NULL,
    [destination_dn]                                   NVARCHAR (30) NULL,
    [start_date_time]                                  DATETIME      NULL,
    [end_date_time]                                    DATETIME      NULL,
    [gmt_offset]                                       SMALLINT      NULL,
    [called_number]                                    NVARCHAR (30) NULL,
    [orig_called_number]                               NVARCHAR (30) NULL,
    [application_name]                                 NVARCHAR (30) NULL,
    [connect_time]                                     SMALLINT      NULL,
    [custom_variable_1]                                VARCHAR (40)  NULL,
    [custom_variable_2]                                VARCHAR (40)  NULL,
    [custom_variable_3]                                VARCHAR (40)  NULL,
    [custom_variable_4]                                VARCHAR (40)  NULL,
    [custom_variable_5]                                VARCHAR (40)  NULL,
    [custom_variable_6]                                VARCHAR (40)  NULL,
    [custom_variable_7]                                VARCHAR (40)  NULL,
    [custom_variable_8]                                VARCHAR (40)  NULL,
    [custom_variable_9]                                VARCHAR (40)  NULL,
    [custom_variable_10]                               VARCHAR (40)  NULL,
    [account_number]                                   VARCHAR (40)  NULL,
    [caller_entered_digits]                            VARCHAR (40)  NULL,
    [bad_call_tag]                                     CHAR (1)      NULL,
    [transfer]                                         BIT           NULL,
    [redirect]                                         BIT           NULL,
    [conference]                                       BIT           NULL,
    [flow_out]                                         BIT           NULL,
    [met_service_level]                                SMALLINT      NULL,
    [campaign_id]                                      INT           NULL,
    [orig_protocol_call_ref]                           VARCHAR (32)  NULL,
    [dest_protocol_call_ref]                           VARCHAR (32)  NULL,
    [call_result]                                      SMALLINT      NULL,
    [dialing_list_id]                                  INT           NULL,
    [dv_load_date_time]                                DATETIME      NOT NULL,
    [dv_r_load_source_id]                              BIGINT        NOT NULL,
    [dv_inserted_date_time]                            DATETIME      NOT NULL,
    [dv_insert_user]                                   VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                             DATETIME      NULL,
    [dv_update_user]                                   VARCHAR (50)  NULL,
    [dv_hash]                                          CHAR (32)     NOT NULL,
    [dv_batch_id]                                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ciscoautoattendant_ipcc_contact_call_detail]
    ON [dbo].[s_ciscoautoattendant_ipcc_contact_call_detail]([bk_hash] ASC, [s_ciscoautoattendant_ipcc_contact_call_detail_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ciscoautoattendant_ipcc_contact_call_detail]([dv_batch_id] ASC);

