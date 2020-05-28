CREATE VIEW [marketing].[v_dim_crm_ltf_live_chat]
AS select d_crmcloudsync_ltf_live_chat.activity_id activity_id,
       d_crmcloudsync_ltf_live_chat.actual_start_dim_date_key actual_start_dim_date_key,
       d_crmcloudsync_ltf_live_chat.description description,
       d_crmcloudsync_ltf_live_chat.dim_club_key dim_club_key,
       d_crmcloudsync_ltf_live_chat.ltf_club_name ltf_club_name,
       d_crmcloudsync_ltf_live_chat.ltf_email_address_1 ltf_email_address_1,
       d_crmcloudsync_ltf_live_chat.ltf_first_name ltf_first_name,
       d_crmcloudsync_ltf_live_chat.ltf_last_name ltf_last_name,
       d_crmcloudsync_ltf_live_chat.ltf_line_of_business ltf_line_of_business,
       d_crmcloudsync_ltf_live_chat.ltf_line_of_business_name ltf_line_of_business_name,
       d_crmcloudsync_ltf_live_chat.ltf_referring_url ltf_referring_url,
       d_crmcloudsync_ltf_live_chat.ltf_transcript ltf_transcript,
       d_crmcloudsync_ltf_live_chat.subject subject
  from dbo.d_crmcloudsync_ltf_live_chat;