CREATE PROC [dbo].[proc_dim_mms_membership_history_bkp] @dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--declare @load_dv_batch_id bigint = (
--    select isnull(min(max_dv_batch_id),-2)
--    from (
--        select max(dv_batch_id) max_dv_batch_id from dim_mms_membership_history
--        --select 20180606070129 max_dv_batch_id
--        --select -1 max_dv_batch_id
--    ) x
--)

create table #eff with (distribution = hash(bk_hash)) as
select bk_hash, effective_date_time,rank() over (partition by bk_hash order by effective_date_time) r
from (
select bk_hash,effective_date_time
from d_mms_membership_snapshot_history
--where dv_batch_id >= @load_dv_batch_id
group by bk_hash,effective_date_time
union
select bk_hash,effective_date_time
from d_mms_membership_history
--where dv_batch_id >= @load_dv_batch_id
group by bk_hash,effective_date_time
) x

create table #exp with (distribution = hash(bk_hash)) as
select e1.bk_hash, e1.effective_date_time, isnull(e2.effective_date_time,'dec 31, 9999') expiration_date_time
from #eff e1
left join #eff e2 on e1.bk_hash = e2.bk_hash and e1.r+1 = e2.r

truncate table dim_mms_membership_history

insert into dim_mms_membership_history (
dim_mms_membership_key,
membership_id,
effective_date_time,
expiration_date_time,
advisor_employee_id,
club_id,
company_id,
created_date_time,
crm_opportunity_id,
current_price,
dim_crm_opportunity_key,
dim_mms_company_key,
dim_mms_membership_type_key,
eft_option_dim_description_key,
enrollment_type_dim_description_key,
home_dim_club_key,
membership_activation_date,
membership_cancellation_request_date,
membership_created_date_time,
membership_created_dim_date_key,
membership_expiration_date,
membership_source_dim_description_key,
membership_status_dim_description_key,
membership_type_id,
non_payment_termination_flag,
original_sales_dim_employee_key,
prior_plus_dim_membership_type_key,
prior_plus_membership_type_id,
prior_plus_price,
termination_reason_club_type_dim_description_key,
termination_reason_dim_description_key,
val_eft_option_id,
val_enrollment_type_id,
val_membership_source_id,
val_membership_status_id,
val_termination_reason_club_type_id,
val_termination_reason_id,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user)

select e.bk_hash dim_mms_membership_key,
       isnull(m.membership_id,s.membership_id) membership_id,
       e.effective_date_time, 
       e.expiration_date_time,
       isnull(m.advisor_employee_id, s.advisor_employee_id) advisor_employee_id,
       isnull(m.club_id, s.club_id) club_id,
       isnull(m.company_id, s.company_id) company_id,
       isnull(m.created_date_time, s.created_date_time) created_date_time,
       isnull(m.crm_opportunity_id, s.crm_opportunity_id) crm_opportunity_id,
       isnull(m.current_price, s.current_price) current_price,
       isnull(m.dim_crm_opportunity_key, s.dim_crm_opportunity_key) dim_crm_opportunity_key,
       isnull(m.dim_mms_company_key, s.dim_mms_company_key) dim_mms_company_key,
       isnull(m.dim_mms_membership_type_key, s.dim_mms_membership_type_key) dim_mms_membership_type_key,
       isnull(m.eft_option_dim_description_key, s.eft_option_dim_description_key) eft_option_dim_description_key,
       isnull(m.enrollment_type_dim_description_key, s.enrollment_type_dim_description_key) enrollment_type_dim_description_key,
       isnull(m.home_dim_club_key, s.home_dim_club_key) home_dim_club_key,
       isnull(m.membership_activation_date, s.membership_activation_date) membership_activation_date,
       isnull(m.membership_cancellation_request_date, s.membership_cancellation_request_date) membership_cancellation_request_date,
       isnull(m.membership_created_date_time, s.membership_created_date_time) membership_created_date_time,
       isnull(m.membership_created_dim_date_key, s.membership_created_dim_date_key) membership_created_dim_date_key,
       isnull(m.membership_expiration_date, s.membership_expiration_date) membership_expiration_date,
       isnull(m.membership_source_dim_description_key, s.membership_source_dim_description_key) membership_source_dim_description_key,
       isnull(m.membership_status_dim_description_key, s.membership_status_dim_description_key) membership_status_dim_description_key,
       isnull(m.membership_type_id, s.membership_type_id) membership_type_id,
       isnull(m.non_payment_termination_flag, s.non_payment_termination_flag) non_payment_termination_flag,
       isnull(m.original_sales_dim_employee_key, s.original_sales_dim_employee_key) original_sales_dim_employee_key,
       isnull(m.prior_plus_dim_membership_type_key, s.prior_plus_dim_membership_type_key) prior_plus_dim_membership_type_key,
       isnull(m.prior_plus_membership_type_id, s.prior_plus_membership_type_id) prior_plus_membership_type_id,
       isnull(m.prior_plus_price, s.prior_plus_price) prior_plus_price,
       isnull(m.termination_reason_club_type_dim_description_key, s.termination_reason_club_type_dim_description_key) termination_reason_club_type_dim_description_key,
       isnull(m.termination_reason_dim_description_key, s.termination_reason_dim_description_key) termination_reason_dim_description_key,
       isnull(m.val_eft_option_id, s.val_eft_option_id) val_eft_option_id,
       isnull(m.val_enrollment_type_id, s.val_enrollment_type_id) val_enrollment_type_id,
       isnull(m.val_membership_source_id, s.val_membership_source_id) val_membership_source_id,
       isnull(m.val_membership_status_id, s.val_membership_status_id) val_membership_status_id,
       isnull(m.val_termination_reason_club_type_id, s.val_termination_reason_club_type_id) val_termination_reason_club_type_id,
       isnull(m.val_termination_reason_id, s.val_termination_reason_id) val_termination_reason_id,
       --case when m.dv_load_date_time >= s.dv_load_date_time then m.dv_load_date_time else s.dv_load_date_time end dv_load_date_time,
       isnull(m.dv_load_date_time, s.dv_load_date_time) dv_load_date_time,
       'dec 31, 9999',
       --case when m.dv_batch_id >= s.dv_batch_id then m.dv_batch_id else s.dv_batch_id end dv_batch_id,
       isnull(m.dv_batch_id, s.dv_batch_id) dv_batch_id,
       getdate(),
       'brian'
from #exp e --29877144

left join d_mms_membership_history m --29877144
  on e.bk_hash = m.bk_hash
 and m.effective_date_time = e.effective_date_time


left join d_mms_membership_snapshot_history s --27476569
  on e.bk_hash = s.bk_hash
 and s.effective_date_time = e.effective_date_time
 



drop table #eff
drop table #exp
--drop table #q

end
