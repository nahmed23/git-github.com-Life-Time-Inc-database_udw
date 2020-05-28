CREATE VIEW [sandbox].[v_membership_history_acquisition]
AS select 'acquisition' record_type
        ,mh.dim_mms_membership_key
       ,cast( mh.membership_created_date_time as date) as date_time
       , cast(mh.effective_date_time as date) effective_date_time, cast(mh.expiration_date_time as date) expiration_date_time
	   , mh.club_id 
from dim_mms_membership_history mh
join dim_club dc on mh.home_dim_club_key = dc.dim_club_key
where 1=1
  and mh.val_membership_status_id not in (1,3)
  and (mh.val_termination_reason_id is null or mh.val_termination_reason_id not in (21,41,42,47,59,73))
  and mh.membership_type_id not in (select 134 membership_type_id --house_account
                                     union
                                    select membership_type_id 
                                      from d_mms_membership_type_attribute
                                     where val_membership_type_attribute_id = 57 --on hold
                                        or val_membership_type_attribute_id = 29 --non_revenue
                                        or val_membership_type_attribute_id in (58,59));