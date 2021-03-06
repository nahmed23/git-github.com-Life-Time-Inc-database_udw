﻿CREATE EXTERNAL TABLE [dbo].[ex_clickstream_raw_myLt] (
    [pandas_index] INT NULL,
    [accept_language] NVARCHAR (MAX) NULL,
    [aemassetid] NVARCHAR (MAX) NULL,
    [aemassetsource] NVARCHAR (MAX) NULL,
    [aemclickedassetid] NVARCHAR (MAX) NULL,
    [browser] NVARCHAR (MAX) NULL,
    [browser_height] NVARCHAR (MAX) NULL,
    [browser_width] NVARCHAR (MAX) NULL,
    [c_color] NVARCHAR (MAX) NULL,
    [campaign] NVARCHAR (MAX) NULL,
    [carrier] NVARCHAR (MAX) NULL,
    [channel] NVARCHAR (MAX) NULL,
    [click_action] NVARCHAR (MAX) NULL,
    [click_action_type] NVARCHAR (MAX) NULL,
    [click_context] NVARCHAR (MAX) NULL,
    [click_context_type] NVARCHAR (MAX) NULL,
    [click_sourceid] NVARCHAR (MAX) NULL,
    [click_tag] NVARCHAR (MAX) NULL,
    [clickmaplink] NVARCHAR (MAX) NULL,
    [clickmaplinkbyregion] NVARCHAR (MAX) NULL,
    [clickmappage] NVARCHAR (MAX) NULL,
    [clickmapregion] NVARCHAR (MAX) NULL,
    [code_ver] NVARCHAR (MAX) NULL,
    [color] NVARCHAR (MAX) NULL,
    [connection_type] NVARCHAR (MAX) NULL,
    [cookies] NVARCHAR (MAX) NULL,
    [country] NVARCHAR (MAX) NULL,
    [ct_connect_type] NVARCHAR (MAX) NULL,
    [curr_factor] NVARCHAR (MAX) NULL,
    [curr_rate] NVARCHAR (MAX) NULL,
    [currency] NVARCHAR (MAX) NULL,
    [cust_hit_time_gmt] NVARCHAR (MAX) NULL,
    [cust_visid] NVARCHAR (MAX) NULL,
    [daily_visitor] NVARCHAR (MAX) NULL,
    [date_time] NVARCHAR (MAX) NULL,
    [domain] NVARCHAR (MAX) NULL,
    [duplicate_events] NVARCHAR (MAX) NULL,
    [duplicate_purchase] NVARCHAR (MAX) NULL,
    [duplicated_from] NVARCHAR (MAX) NULL,
    [ef_id] NVARCHAR (MAX) NULL,
    [evar1] NVARCHAR (MAX) NULL,
    [evar2] NVARCHAR (MAX) NULL,
    [evar3] NVARCHAR (MAX) NULL,
    [evar4] NVARCHAR (MAX) NULL,
    [evar5] NVARCHAR (MAX) NULL,
    [evar6] NVARCHAR (MAX) NULL,
    [evar7] NVARCHAR (MAX) NULL,
    [evar8] NVARCHAR (MAX) NULL,
    [evar9] NVARCHAR (MAX) NULL,
    [evar10] NVARCHAR (MAX) NULL,
    [evar11] NVARCHAR (MAX) NULL,
    [evar12] NVARCHAR (MAX) NULL,
    [evar13] NVARCHAR (MAX) NULL,
    [evar14] NVARCHAR (MAX) NULL,
    [evar15] NVARCHAR (MAX) NULL,
    [evar16] NVARCHAR (MAX) NULL,
    [evar17] NVARCHAR (MAX) NULL,
    [evar18] NVARCHAR (MAX) NULL,
    [evar19] NVARCHAR (MAX) NULL,
    [evar20] NVARCHAR (MAX) NULL,
    [evar21] NVARCHAR (MAX) NULL,
    [evar22] NVARCHAR (MAX) NULL,
    [evar23] NVARCHAR (MAX) NULL,
    [evar24] NVARCHAR (MAX) NULL,
    [evar25] NVARCHAR (MAX) NULL,
    [evar26] NVARCHAR (MAX) NULL,
    [evar28] NVARCHAR (MAX) NULL,
    [evar29] NVARCHAR (MAX) NULL,
    [evar30] NVARCHAR (MAX) NULL,
    [evar31] NVARCHAR (MAX) NULL,
    [evar32] NVARCHAR (MAX) NULL,
    [evar33] NVARCHAR (MAX) NULL,
    [evar34] NVARCHAR (MAX) NULL,
    [evar35] NVARCHAR (MAX) NULL,
    [evar36] NVARCHAR (MAX) NULL,
    [evar37] NVARCHAR (MAX) NULL,
    [evar38] NVARCHAR (MAX) NULL,
    [evar39] NVARCHAR (MAX) NULL,
    [evar40] NVARCHAR (MAX) NULL,
    [evar41] NVARCHAR (MAX) NULL,
    [evar42] NVARCHAR (MAX) NULL,
    [evar43] NVARCHAR (MAX) NULL,
    [evar44] NVARCHAR (MAX) NULL,
    [evar45] NVARCHAR (MAX) NULL,
    [evar46] NVARCHAR (MAX) NULL,
    [evar47] NVARCHAR (MAX) NULL,
    [evar48] NVARCHAR (MAX) NULL,
    [evar49] NVARCHAR (MAX) NULL,
    [evar50] NVARCHAR (MAX) NULL,
    [evar51] NVARCHAR (MAX) NULL,
    [evar52] NVARCHAR (MAX) NULL,
    [evar53] NVARCHAR (MAX) NULL,
    [evar54] NVARCHAR (MAX) NULL,
    [evar55] NVARCHAR (MAX) NULL,
    [evar56] NVARCHAR (MAX) NULL,
    [evar57] NVARCHAR (MAX) NULL,
    [evar58] NVARCHAR (MAX) NULL,
    [evar59] NVARCHAR (MAX) NULL,
    [evar61] NVARCHAR (MAX) NULL,
    [evar63] NVARCHAR (MAX) NULL,
    [evar71] NVARCHAR (MAX) NULL,
    [evar72] NVARCHAR (MAX) NULL,
    [evar74] NVARCHAR (MAX) NULL,
    [evar75] NVARCHAR (MAX) NULL,
    [evar77] NVARCHAR (MAX) NULL,
    [evar78] NVARCHAR (MAX) NULL,
    [evar79] NVARCHAR (MAX) NULL,
    [evar80] NVARCHAR (MAX) NULL,
    [evar81] NVARCHAR (MAX) NULL,
    [evar82] NVARCHAR (MAX) NULL,
    [evar83] NVARCHAR (MAX) NULL,
    [evar84] NVARCHAR (MAX) NULL,
    [evar85] NVARCHAR (MAX) NULL,
    [evar86] NVARCHAR (MAX) NULL,
    [evar87] NVARCHAR (MAX) NULL,
    [evar88] NVARCHAR (MAX) NULL,
    [evar89] NVARCHAR (MAX) NULL,
    [evar90] NVARCHAR (MAX) NULL,
    [evar91] NVARCHAR (MAX) NULL,
    [evar92] NVARCHAR (MAX) NULL,
    [evar93] NVARCHAR (MAX) NULL,
    [evar94] NVARCHAR (MAX) NULL,
    [evar95] NVARCHAR (MAX) NULL,
    [evar96] NVARCHAR (MAX) NULL,
    [evar97] NVARCHAR (MAX) NULL,
    [evar98] NVARCHAR (MAX) NULL,
    [evar99] NVARCHAR (MAX) NULL,
    [evar100] NVARCHAR (MAX) NULL,
    [evar101] NVARCHAR (MAX) NULL,
    [evar102] NVARCHAR (MAX) NULL,
    [evar103] NVARCHAR (MAX) NULL,
    [evar104] NVARCHAR (MAX) NULL,
    [evar105] NVARCHAR (MAX) NULL,
    [evar106] NVARCHAR (MAX) NULL,
    [evar107] NVARCHAR (MAX) NULL,
    [evar108] NVARCHAR (MAX) NULL,
    [evar109] NVARCHAR (MAX) NULL,
    [evar110] NVARCHAR (MAX) NULL,
    [evar111] NVARCHAR (MAX) NULL,
    [evar112] NVARCHAR (MAX) NULL,
    [evar113] NVARCHAR (MAX) NULL,
    [evar114] NVARCHAR (MAX) NULL,
    [evar115] NVARCHAR (MAX) NULL,
    [evar116] NVARCHAR (MAX) NULL,
    [evar117] NVARCHAR (MAX) NULL,
    [evar118] NVARCHAR (MAX) NULL,
    [evar119] NVARCHAR (MAX) NULL,
    [evar120] NVARCHAR (MAX) NULL,
    [evar121] NVARCHAR (MAX) NULL,
    [evar122] NVARCHAR (MAX) NULL,
    [evar123] NVARCHAR (MAX) NULL,
    [evar124] NVARCHAR (MAX) NULL,
    [evar125] NVARCHAR (MAX) NULL,
    [evar126] NVARCHAR (MAX) NULL,
    [evar127] NVARCHAR (MAX) NULL,
    [evar128] NVARCHAR (MAX) NULL,
    [evar129] NVARCHAR (MAX) NULL,
    [evar130] NVARCHAR (MAX) NULL,
    [evar131] NVARCHAR (MAX) NULL,
    [evar132] NVARCHAR (MAX) NULL,
    [evar133] NVARCHAR (MAX) NULL,
    [evar134] NVARCHAR (MAX) NULL,
    [evar135] NVARCHAR (MAX) NULL,
    [evar136] NVARCHAR (MAX) NULL,
    [evar137] NVARCHAR (MAX) NULL,
    [evar138] NVARCHAR (MAX) NULL,
    [evar139] NVARCHAR (MAX) NULL,
    [evar140] NVARCHAR (MAX) NULL,
    [evar141] NVARCHAR (MAX) NULL,
    [evar142] NVARCHAR (MAX) NULL,
    [evar143] NVARCHAR (MAX) NULL,
    [evar144] NVARCHAR (MAX) NULL,
    [evar145] NVARCHAR (MAX) NULL,
    [evar146] NVARCHAR (MAX) NULL,
    [evar147] NVARCHAR (MAX) NULL,
    [evar148] NVARCHAR (MAX) NULL,
    [evar149] NVARCHAR (MAX) NULL,
    [evar150] NVARCHAR (MAX) NULL,
    [evar151] NVARCHAR (MAX) NULL,
    [evar152] NVARCHAR (MAX) NULL,
    [evar153] NVARCHAR (MAX) NULL,
    [evar154] NVARCHAR (MAX) NULL,
    [evar155] NVARCHAR (MAX) NULL,
    [evar156] NVARCHAR (MAX) NULL,
    [evar157] NVARCHAR (MAX) NULL,
    [evar166] NVARCHAR (MAX) NULL,
    [evar199] NVARCHAR (MAX) NULL,
    [evar201] NVARCHAR (MAX) NULL,
    [evar202] NVARCHAR (MAX) NULL,
    [evar203] NVARCHAR (MAX) NULL,
    [evar204] NVARCHAR (MAX) NULL,
    [evar205] NVARCHAR (MAX) NULL,
    [evar206] NVARCHAR (MAX) NULL,
    [evar207] NVARCHAR (MAX) NULL,
    [evar208] NVARCHAR (MAX) NULL,
    [evar209] NVARCHAR (MAX) NULL,
    [evar210] NVARCHAR (MAX) NULL,
    [evar211] NVARCHAR (MAX) NULL,
    [evar212] NVARCHAR (MAX) NULL,
    [evar213] NVARCHAR (MAX) NULL,
    [evar214] NVARCHAR (MAX) NULL,
    [evar215] NVARCHAR (MAX) NULL,
    [evar216] NVARCHAR (MAX) NULL,
    [evar232] NVARCHAR (MAX) NULL,
    [evar235] NVARCHAR (MAX) NULL,
    [evar236] NVARCHAR (MAX) NULL,
    [evar238] NVARCHAR (MAX) NULL,
    [evar239] NVARCHAR (MAX) NULL,
    [evar240] NVARCHAR (MAX) NULL,
    [evar241] NVARCHAR (MAX) NULL,
    [evar242] NVARCHAR (MAX) NULL,
    [evar243] NVARCHAR (MAX) NULL,
    [evar244] NVARCHAR (MAX) NULL,
    [evar245] NVARCHAR (MAX) NULL,
    [evar246] NVARCHAR (MAX) NULL,
    [evar247] NVARCHAR (MAX) NULL,
    [evar248] NVARCHAR (MAX) NULL,
    [evar249] NVARCHAR (MAX) NULL,
    [evar250] NVARCHAR (MAX) NULL,
    [event_list] NVARCHAR (MAX) NULL,
    [exclude_hit] NVARCHAR (MAX) NULL,
    [first_hit_page_url] NVARCHAR (MAX) NULL,
    [first_hit_pagename] NVARCHAR (MAX) NULL,
    [first_hit_ref_domain] NVARCHAR (MAX) NULL,
    [first_hit_ref_type] NVARCHAR (MAX) NULL,
    [first_hit_referrer] NVARCHAR (MAX) NULL,
    [first_hit_time_gmt] NVARCHAR (MAX) NULL,
    [geo_city] NVARCHAR (MAX) NULL,
    [geo_country] NVARCHAR (MAX) NULL,
    [geo_dma] NVARCHAR (MAX) NULL,
    [geo_region] NVARCHAR (MAX) NULL,
    [geo_zip] NVARCHAR (MAX) NULL,
    [hier1] NVARCHAR (MAX) NULL,
    [hier2] NVARCHAR (MAX) NULL,
    [hier3] NVARCHAR (MAX) NULL,
    [hier4] NVARCHAR (MAX) NULL,
    [hier5] NVARCHAR (MAX) NULL,
    [hit_source] NVARCHAR (MAX) NULL,
    [hit_time_gmt] NVARCHAR (MAX) NULL,
    [hitid_high] NVARCHAR (MAX) NULL,
    [hitid_low] NVARCHAR (MAX) NULL,
    [homepage] NVARCHAR (MAX) NULL,
    [hourly_visitor] NVARCHAR (MAX) NULL,
    [ip] NVARCHAR (MAX) NULL,
    [j_jscript] NVARCHAR (MAX) NULL,
    [java_enabled] NVARCHAR (MAX) NULL,
    [javascript] NVARCHAR (MAX) NULL,
    [language] NVARCHAR (MAX) NULL,
    [last_hit_time_gmt] NVARCHAR (MAX) NULL,
    [last_purchase_num] NVARCHAR (MAX) NULL,
    [last_purchase_time_gmt] NVARCHAR (MAX) NULL,
    [mcvisid] NVARCHAR (MAX) NULL,
    [mobile_id] NVARCHAR (MAX) NULL,
    [mobileactioninapptime] NVARCHAR (MAX) NULL,
    [mobileactiontotaltime] NVARCHAR (MAX) NULL,
    [mobileappid] NVARCHAR (MAX) NULL,
    [mobileappperformanceaffectedusers] NVARCHAR (MAX) NULL,
    [mobileappperformanceappid] NVARCHAR (MAX) NULL,
    [mobileupgrades] NVARCHAR (MAX) NULL,
    [monthly_visitor] NVARCHAR (MAX) NULL,
    [mvvar2] NVARCHAR (MAX) NULL,
    [mvvar3] NVARCHAR (MAX) NULL,
    [namespace] NVARCHAR (MAX) NULL,
    [new_visit] NVARCHAR (MAX) NULL,
    [os] NVARCHAR (MAX) NULL,
    [page_event] NVARCHAR (MAX) NULL,
    [page_event_var1] NVARCHAR (MAX) NULL,
    [page_event_var2] NVARCHAR (MAX) NULL,
    [page_event_var3] NVARCHAR (MAX) NULL,
    [page_type] NVARCHAR (MAX) NULL,
    [page_url] NVARCHAR (MAX) NULL,
    [pagename] NVARCHAR (MAX) NULL,
    [paid_search] NVARCHAR (MAX) NULL,
    [partner_plugins] NVARCHAR (MAX) NULL,
    [persistent_cookie] NVARCHAR (MAX) NULL,
    [plugins] NVARCHAR (MAX) NULL,
    [pointofinterestdistance] NVARCHAR (MAX) NULL,
    [post_browser_height] NVARCHAR (MAX) NULL,
    [post_browser_width] NVARCHAR (MAX) NULL,
    [post_campaign] NVARCHAR (MAX) NULL,
    [post_channel] NVARCHAR (MAX) NULL,
    [post_cookies] NVARCHAR (MAX) NULL,
    [post_currency] NVARCHAR (MAX) NULL,
    [post_cust_hit_time_gmt] NVARCHAR (MAX) NULL,
    [post_cust_visid] NVARCHAR (MAX) NULL,
    [post_ef_id] NVARCHAR (MAX) NULL,
    [post_evar1] NVARCHAR (MAX) NULL,
    [post_evar2] NVARCHAR (MAX) NULL,
    [post_evar3] NVARCHAR (MAX) NULL,
    [post_evar4] NVARCHAR (MAX) NULL,
    [post_evar5] NVARCHAR (MAX) NULL,
    [post_evar6] NVARCHAR (MAX) NULL,
    [post_evar7] NVARCHAR (MAX) NULL,
    [post_evar8] NVARCHAR (MAX) NULL,
    [post_evar9] NVARCHAR (MAX) NULL,
    [post_evar10] NVARCHAR (MAX) NULL,
    [post_evar11] NVARCHAR (MAX) NULL,
    [post_evar12] NVARCHAR (MAX) NULL,
    [post_evar13] NVARCHAR (MAX) NULL,
    [post_evar14] NVARCHAR (MAX) NULL,
    [post_evar15] NVARCHAR (MAX) NULL,
    [post_evar16] NVARCHAR (MAX) NULL,
    [post_evar18] NVARCHAR (MAX) NULL,
    [post_evar19] NVARCHAR (MAX) NULL,
    [post_evar20] NVARCHAR (MAX) NULL,
    [post_evar22] NVARCHAR (MAX) NULL,
    [post_evar23] NVARCHAR (MAX) NULL,
    [post_evar24] NVARCHAR (MAX) NULL,
    [post_evar29] NVARCHAR (MAX) NULL,
    [post_evar30] NVARCHAR (MAX) NULL,
    [post_evar31] NVARCHAR (MAX) NULL,
    [post_evar33] NVARCHAR (MAX) NULL,
    [post_evar34] NVARCHAR (MAX) NULL,
    [post_evar35] NVARCHAR (MAX) NULL,
    [post_evar39] NVARCHAR (MAX) NULL,
    [post_evar50] NVARCHAR (MAX) NULL,
    [post_evar240] NVARCHAR (MAX) NULL,
    [post_evar246] NVARCHAR (MAX) NULL,
    [post_event_list] NVARCHAR (MAX) NULL,
    [post_java_enabled] NVARCHAR (MAX) NULL,
    [post_keywords] NVARCHAR (MAX) NULL,
    [post_mobilelaunchnumber] NVARCHAR (MAX) NULL,
    [post_mobilemessageid] NVARCHAR (MAX) NULL,
    [post_mobilepushoptin] NVARCHAR (MAX) NULL,
    [post_mobilepushpayloadid] NVARCHAR (MAX) NULL,
    [post_mobileresolution] NVARCHAR (MAX) NULL,
    [post_mvvar2] NVARCHAR (MAX) NULL,
    [post_page_event] NVARCHAR (MAX) NULL,
    [post_page_event_var1] NVARCHAR (MAX) NULL,
    [post_page_event_var2] NVARCHAR (MAX) NULL,
    [post_page_event_var3] NVARCHAR (MAX) NULL,
    [post_page_type] NVARCHAR (MAX) NULL,
    [post_page_url] NVARCHAR (MAX) NULL,
    [post_pagename] NVARCHAR (MAX) NULL,
    [post_pagename_no_url] NVARCHAR (MAX) NULL,
    [post_persistent_cookie] NVARCHAR (MAX) NULL,
    [post_pointofinterest] NVARCHAR (MAX) NULL,
    [post_product_list] NVARCHAR (MAX) NULL,
    [post_prop1] NVARCHAR (MAX) NULL,
    [post_prop2] NVARCHAR (MAX) NULL,
    [post_prop3] NVARCHAR (MAX) NULL,
    [post_prop4] NVARCHAR (MAX) NULL,
    [post_prop5] NVARCHAR (MAX) NULL,
    [post_prop6] NVARCHAR (MAX) NULL,
    [post_prop7] NVARCHAR (MAX) NULL,
    [post_prop8] NVARCHAR (MAX) NULL,
    [post_prop9] NVARCHAR (MAX) NULL,
    [post_prop10] NVARCHAR (MAX) NULL,
    [post_prop11] NVARCHAR (MAX) NULL,
    [post_prop12] NVARCHAR (MAX) NULL,
    [post_prop13] NVARCHAR (MAX) NULL,
    [post_prop14] NVARCHAR (MAX) NULL,
    [post_prop15] NVARCHAR (MAX) NULL,
    [post_prop16] NVARCHAR (MAX) NULL,
    [post_prop17] NVARCHAR (MAX) NULL,
    [post_prop18] NVARCHAR (MAX) NULL,
    [post_prop19] NVARCHAR (MAX) NULL,
    [post_prop20] NVARCHAR (MAX) NULL,
    [post_prop29] NVARCHAR (MAX) NULL,
    [post_prop39] NVARCHAR (MAX) NULL,
    [post_prop40] NVARCHAR (MAX) NULL,
    [post_prop41] NVARCHAR (MAX) NULL,
    [post_prop42] NVARCHAR (MAX) NULL,
    [post_prop50] NVARCHAR (MAX) NULL,
    [post_prop51] NVARCHAR (MAX) NULL,
    [post_prop52] NVARCHAR (MAX) NULL,
    [post_prop53] NVARCHAR (MAX) NULL,
    [post_prop54] NVARCHAR (MAX) NULL,
    [post_prop55] NVARCHAR (MAX) NULL,
    [post_prop56] NVARCHAR (MAX) NULL,
    [post_prop57] NVARCHAR (MAX) NULL,
    [post_prop58] NVARCHAR (MAX) NULL,
    [post_prop59] NVARCHAR (MAX) NULL,
    [post_prop60] NVARCHAR (MAX) NULL,
    [post_prop61] NVARCHAR (MAX) NULL,
    [post_prop62] NVARCHAR (MAX) NULL,
    [post_prop66] NVARCHAR (MAX) NULL,
    [post_prop68] NVARCHAR (MAX) NULL,
    [post_purchaseid] NVARCHAR (MAX) NULL,
    [post_referrer] NVARCHAR (MAX) NULL,
    [post_search_engine] NVARCHAR (MAX) NULL,
    [post_socialcontentprovider] NVARCHAR (MAX) NULL,
    [post_socialproperty_deprecated] NVARCHAR (MAX) NULL,
    [post_socialpubcomments] NVARCHAR (MAX) NULL,
    [post_t_time_info] NVARCHAR (MAX) NULL,
    [post_tnt] NVARCHAR (MAX) NULL,
    [post_tnt_action] NVARCHAR (MAX) NULL,
    [post_videoname] NVARCHAR (MAX) NULL,
    [post_videopath] NVARCHAR (MAX) NULL,
    [post_videoplayername] NVARCHAR (MAX) NULL,
    [post_videoqoebitrateaverageevar] NVARCHAR (MAX) NULL,
    [post_videoqoebitratechangecountevar] NVARCHAR (MAX) NULL,
    [post_videoqoedroppedframecountevar] NVARCHAR (MAX) NULL,
    [post_videoqoeerrorcountevar] NVARCHAR (MAX) NULL,
    [post_videoqoetimetostartevar] NVARCHAR (MAX) NULL,
    [post_videosegment] NVARCHAR (MAX) NULL,
    [post_visid_high] NVARCHAR (MAX) NULL,
    [post_visid_low] NVARCHAR (MAX) NULL,
    [post_visid_type] NVARCHAR (MAX) NULL,
    [post_zip] NVARCHAR (MAX) NULL,
    [prev_page] NVARCHAR (MAX) NULL,
    [product_list] NVARCHAR (MAX) NULL,
    [prop1] NVARCHAR (MAX) NULL,
    [prop2] NVARCHAR (MAX) NULL,
    [prop3] NVARCHAR (MAX) NULL,
    [prop4] NVARCHAR (MAX) NULL,
    [prop5] NVARCHAR (MAX) NULL,
    [prop6] NVARCHAR (MAX) NULL,
    [prop7] NVARCHAR (MAX) NULL,
    [prop8] NVARCHAR (MAX) NULL,
    [prop9] NVARCHAR (MAX) NULL,
    [prop10] NVARCHAR (MAX) NULL,
    [prop11] NVARCHAR (MAX) NULL,
    [prop12] NVARCHAR (MAX) NULL,
    [prop13] NVARCHAR (MAX) NULL,
    [prop14] NVARCHAR (MAX) NULL,
    [prop15] NVARCHAR (MAX) NULL,
    [prop16] NVARCHAR (MAX) NULL,
    [prop17] NVARCHAR (MAX) NULL,
    [prop18] NVARCHAR (MAX) NULL,
    [prop19] NVARCHAR (MAX) NULL,
    [prop20] NVARCHAR (MAX) NULL,
    [prop29] NVARCHAR (MAX) NULL,
    [prop40] NVARCHAR (MAX) NULL,
    [prop41] NVARCHAR (MAX) NULL,
    [prop42] NVARCHAR (MAX) NULL,
    [prop51] NVARCHAR (MAX) NULL,
    [prop52] NVARCHAR (MAX) NULL,
    [prop53] NVARCHAR (MAX) NULL,
    [prop54] NVARCHAR (MAX) NULL,
    [prop55] NVARCHAR (MAX) NULL,
    [prop56] NVARCHAR (MAX) NULL,
    [prop57] NVARCHAR (MAX) NULL,
    [prop58] NVARCHAR (MAX) NULL,
    [prop59] NVARCHAR (MAX) NULL,
    [prop60] NVARCHAR (MAX) NULL,
    [prop61] NVARCHAR (MAX) NULL,
    [prop62] NVARCHAR (MAX) NULL,
    [prop66] NVARCHAR (MAX) NULL,
    [prop67] NVARCHAR (MAX) NULL,
    [prop68] NVARCHAR (MAX) NULL,
    [prop69] NVARCHAR (MAX) NULL,
    [prop70] NVARCHAR (MAX) NULL,
    [prop72] NVARCHAR (MAX) NULL,
    [prop73] NVARCHAR (MAX) NULL,
    [prop74] NVARCHAR (MAX) NULL,
    [prop75] NVARCHAR (MAX) NULL,
    [purchaseid] NVARCHAR (MAX) NULL,
    [quarterly_visitor] NVARCHAR (MAX) NULL,
    [ref_domain] NVARCHAR (MAX) NULL,
    [ref_type] NVARCHAR (MAX) NULL,
    [referrer] NVARCHAR (MAX) NULL,
    [resolution] NVARCHAR (MAX) NULL,
    [s_resolution] NVARCHAR (MAX) NULL,
    [sampled_hit] NVARCHAR (MAX) NULL,
    [search_engine] NVARCHAR (MAX) NULL,
    [search_page_num] NVARCHAR (MAX) NULL,
    [secondary_hit] NVARCHAR (MAX) NULL,
    [service] NVARCHAR (MAX) NULL,
    [socialpostviews] NVARCHAR (MAX) NULL,
    [socialproperty_deprecated] NVARCHAR (MAX) NULL,
    [socialpubcomments] NVARCHAR (MAX) NULL,
    [socialtermslist] NVARCHAR (MAX) NULL,
    [sourceid] NVARCHAR (MAX) NULL,
    [state] NVARCHAR (MAX) NULL,
    [stats_server] NVARCHAR (MAX) NULL,
    [t_time_info] NVARCHAR (MAX) NULL,
    [tnt] NVARCHAR (MAX) NULL,
    [tnt_action] NVARCHAR (MAX) NULL,
    [tnt_post_vista] NVARCHAR (MAX) NULL,
    [transactionid] NVARCHAR (MAX) NULL,
    [truncated_hit] NVARCHAR (MAX) NULL,
    [ua_color] NVARCHAR (MAX) NULL,
    [ua_os] NVARCHAR (MAX) NULL,
    [ua_pixels] NVARCHAR (MAX) NULL,
    [user_agent] NVARCHAR (MAX) NULL,
    [user_hash] NVARCHAR (MAX) NULL,
    [userid] NVARCHAR (MAX) NULL,
    [username] NVARCHAR (MAX) NULL,
    [va_closer_detail] NVARCHAR (MAX) NULL,
    [va_closer_id] NVARCHAR (MAX) NULL,
    [va_finder_detail] NVARCHAR (MAX) NULL,
    [va_finder_id] NVARCHAR (MAX) NULL,
    [va_instance_event] NVARCHAR (MAX) NULL,
    [va_new_engagement] NVARCHAR (MAX) NULL,
    [videoname] NVARCHAR (MAX) NULL,
    [videopath] NVARCHAR (MAX) NULL,
    [videoplayername] NVARCHAR (MAX) NULL,
    [videoqoebitrateaverageevar] NVARCHAR (MAX) NULL,
    [videoqoebitratechangecountevar] NVARCHAR (MAX) NULL,
    [videoqoebuffercountevar] NVARCHAR (MAX) NULL,
    [videoqoebuffertimeevar] NVARCHAR (MAX) NULL,
    [videoqoedroppedframecountevar] NVARCHAR (MAX) NULL,
    [videoqoeerrorcountevar] NVARCHAR (MAX) NULL,
    [videoqoetimetostartevar] NVARCHAR (MAX) NULL,
    [videosegment] NVARCHAR (MAX) NULL,
    [visid_high] NVARCHAR (MAX) NULL,
    [visid_low] NVARCHAR (MAX) NULL,
    [visid_new] NVARCHAR (MAX) NULL,
    [visid_timestamp] NVARCHAR (MAX) NULL,
    [visid_type] NVARCHAR (MAX) NULL,
    [visit_keywords] NVARCHAR (MAX) NULL,
    [visit_num] NVARCHAR (MAX) NULL,
    [visit_page_num] NVARCHAR (MAX) NULL,
    [visit_ref_domain] NVARCHAR (MAX) NULL,
    [visit_ref_type] NVARCHAR (MAX) NULL,
    [visit_referrer] NVARCHAR (MAX) NULL,
    [visit_search_engine] NVARCHAR (MAX) NULL,
    [visit_start_page_url] NVARCHAR (MAX) NULL,
    [visit_start_pagename] NVARCHAR (MAX) NULL,
    [visit_start_time_gmt] NVARCHAR (MAX) NULL,
    [weekly_visitor] NVARCHAR (MAX) NULL,
    [yearly_visitor] NVARCHAR (MAX) NULL
)
    WITH (
    DATA_SOURCE = [dl_ext_source_bidistributeddatablob_rawzone_clickstream],
    LOCATION = N'/mylt',
    FILE_FORMAT = [dl_file_format_clickstream_mylt],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

