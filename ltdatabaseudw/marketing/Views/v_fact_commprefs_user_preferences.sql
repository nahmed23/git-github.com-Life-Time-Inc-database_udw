﻿CREATE VIEW [marketing].[v_fact_commprefs_user_preferences] AS select fact_commprefs_user_preferences.ae_last_sent_date ae_last_sent_date,
       fact_commprefs_user_preferences.ae_total_sent ae_total_sent,
       fact_commprefs_user_preferences.el_last_sent_date el_last_sent_date,
       fact_commprefs_user_preferences.el_total_sent el_total_sent,
       fact_commprefs_user_preferences.email_address email_address,
       fact_commprefs_user_preferences.first_sent_date first_sent_date,
       fact_commprefs_user_preferences.flourish_opt_in flourish_opt_in,
       fact_commprefs_user_preferences.global_opt_in global_opt_in,
       fact_commprefs_user_preferences.global_opt_status_from_date global_opt_status_from_date,
       fact_commprefs_user_preferences.invalid_bounce invalid_bounce,
       fact_commprefs_user_preferences.invalid_bounce_date invalid_bounce_date,
       fact_commprefs_user_preferences.last_bounce_date last_bounce_date,
       fact_commprefs_user_preferences.last_click_date last_click_date,
       fact_commprefs_user_preferences.last_engagement_date last_engagement_date,
       fact_commprefs_user_preferences.last_open_date last_open_date,
       fact_commprefs_user_preferences.last_sent_date last_sent_date,
       fact_commprefs_user_preferences.lt_insider_opt_in lt_insider_opt_in,
       fact_commprefs_user_preferences.no_engagement_12_months no_engagement_12_months,
       fact_commprefs_user_preferences.no_engagement_13_months no_engagement_13_months,
       fact_commprefs_user_preferences.no_engagement_3_months no_engagement_3_months,
       fact_commprefs_user_preferences.no_engagement_6_months no_engagement_6_months,
       fact_commprefs_user_preferences.notifications_opt_in notifications_opt_in,
       fact_commprefs_user_preferences.promotional_opt_in promotional_opt_in,
       fact_commprefs_user_preferences.total_hard_bounces total_hard_bounces,
       fact_commprefs_user_preferences.total_sent total_sent,
       fact_commprefs_user_preferences.total_sent_after_last_engage total_sent_after_last_engage
  from dbo.fact_commprefs_user_preferences;