CREATE VIEW [sandbox].[v_membership_history_hold_request]
AS select 'to on hold' record_type
        ,mh.dim_mms_membership_key
       ,cast( r.request_date_time as date) as date_time
	   , mh.club_id
from d_mms_membership_modification_request r
join dim_mms_membership_history mh 
  on r.dim_mms_membership_key = mh.dim_mms_membership_key
 and mh.effective_date_time <= dateadd(dd,1,convert(datetime,convert(Varchar,r.request_date_time,110),110))
 and mh.expiration_date_time > dateadd(dd,1,convert(datetime,convert(Varchar,r.request_date_time,110),110))
where r.val_membership_modification_request_type_id = 3
 and r.val_membership_modification_request_status_id <> 3
 and (mh.val_termination_reason_id is null or mh.val_termination_reason_id not in (21,41,42,47,59,73))
 and mh.membership_type_id not in (select 134 membership_type_id --house_account
                                     union
                                    select membership_type_id 
                                      from d_mms_membership_type_attribute
                                     where val_membership_type_attribute_id = 57 --on hold
                                        or val_membership_type_attribute_id = 29 --non_revenue
                                        or val_membership_type_attribute_id in (58,59));