CREATE PROC [dbo].[proc_etl_boss_asi_reserv] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asireserv

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asireserv (
       bk_hash,
       reservation,
       trainer_cust_code,
       trainer_mbr_code,
       upccode,
       reserve_type,
       start_date,
       end_date,
       session_id,
       program_id,
       def_price,
       instructor,
       billing_count,
       status,
       free_date,
       qoh,
       limit,
       recurring,
       color,
       shape,
       comment,
       club,
       resource_id,
       resource,
       create_date,
       link_to,
       min_limit,
       non_mbr_price,
       ical_recur_rule,
       upc_desc,
       respect_holidays,
       interest_id,
       origin,
       print_desc,
       day_plan_string,
       day_plan_ints,
       publish,
       published_duration,
       format_id,
       mms_product_id,
       web_register,
       target,
       class_expense,
       instructor_expense,
       age_low,
       age_high,
       web_purchase,
       payment_freq,
       last_modified,
       waiver_reqd,
       deposit_amt,
       payment_reqd_days,
       pre_assgn_instr_cnt,
       grace_days,
       continuous,
       allow_waitlist,
       use_for_LTBucks,
       cancel_dates,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(reservation as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       reservation,
       trainer_cust_code,
       trainer_mbr_code,
       upccode,
       reserve_type,
       start_date,
       end_date,
       session_id,
       program_id,
       def_price,
       instructor,
       billing_count,
       status,
       free_date,
       qoh,
       limit,
       recurring,
       color,
       shape,
       comment,
       club,
       resource_id,
       resource,
       create_date,
       link_to,
       min_limit,
       non_mbr_price,
       ical_recur_rule,
       upc_desc,
       respect_holidays,
       interest_id,
       origin,
       print_desc,
       day_plan_string,
       day_plan_ints,
       publish,
       published_duration,
       format_id,
       mms_product_id,
       web_register,
       target,
       class_expense,
       instructor_expense,
       age_low,
       age_high,
       web_purchase,
       payment_freq,
       last_modified,
       waiver_reqd,
       deposit_amt,
       payment_reqd_days,
       pre_assgn_instr_cnt,
       grace_days,
       continuous,
       allow_waitlist,
       use_for_LTBucks,
       cancel_dates,
       isnull(cast(stage_boss_asireserv.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_boss_asireserv
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_reserv @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_reserv (
       bk_hash,
       reservation,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asireserv.bk_hash,
       stage_hash_boss_asireserv.reservation reservation,
       isnull(cast(stage_hash_boss_asireserv.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asireserv
  left join h_boss_asi_reserv
    on stage_hash_boss_asireserv.bk_hash = h_boss_asi_reserv.bk_hash
 where h_boss_asi_reserv_id is null
   and stage_hash_boss_asireserv.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_reserv
if object_id('tempdb..#l_boss_asi_reserv_inserts') is not null drop table #l_boss_asi_reserv_inserts
create table #l_boss_asi_reserv_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asireserv.bk_hash,
       stage_hash_boss_asireserv.reservation reservation,
       stage_hash_boss_asireserv.trainer_cust_code trainer_cust_code,
       stage_hash_boss_asireserv.upccode upc_code,
       stage_hash_boss_asireserv.club club,
       stage_hash_boss_asireserv.resource_id resource_id,
       stage_hash_boss_asireserv.link_to link_to,
       stage_hash_boss_asireserv.interest_id interest_id,
       stage_hash_boss_asireserv.format_id format_id,
       stage_hash_boss_asireserv.mms_product_id mms_product_id,
       isnull(cast(stage_hash_boss_asireserv.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.reservation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.trainer_cust_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.upccode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.club as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.resource_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.link_to as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.interest_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.format_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.mms_product_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asireserv
 where stage_hash_boss_asireserv.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_reserv records
set @insert_date_time = getdate()
insert into l_boss_asi_reserv (
       bk_hash,
       reservation,
       trainer_cust_code,
       upc_code,
       club,
       resource_id,
       link_to,
       interest_id,
       format_id,
       mms_product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_reserv_inserts.bk_hash,
       #l_boss_asi_reserv_inserts.reservation,
       #l_boss_asi_reserv_inserts.trainer_cust_code,
       #l_boss_asi_reserv_inserts.upc_code,
       #l_boss_asi_reserv_inserts.club,
       #l_boss_asi_reserv_inserts.resource_id,
       #l_boss_asi_reserv_inserts.link_to,
       #l_boss_asi_reserv_inserts.interest_id,
       #l_boss_asi_reserv_inserts.format_id,
       #l_boss_asi_reserv_inserts.mms_product_id,
       case when l_boss_asi_reserv.l_boss_asi_reserv_id is null then isnull(#l_boss_asi_reserv_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_reserv_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_reserv_inserts
  left join p_boss_asi_reserv
    on #l_boss_asi_reserv_inserts.bk_hash = p_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_reserv
    on p_boss_asi_reserv.bk_hash = l_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.l_boss_asi_reserv_id = l_boss_asi_reserv.l_boss_asi_reserv_id
 where l_boss_asi_reserv.l_boss_asi_reserv_id is null
    or (l_boss_asi_reserv.l_boss_asi_reserv_id is not null
        and l_boss_asi_reserv.dv_hash <> #l_boss_asi_reserv_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_reserv
if object_id('tempdb..#s_boss_asi_reserv_inserts') is not null drop table #s_boss_asi_reserv_inserts
create table #s_boss_asi_reserv_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asireserv.bk_hash,
       stage_hash_boss_asireserv.reservation reservation,
       stage_hash_boss_asireserv.trainer_mbr_code trainer_mbr_code,
       stage_hash_boss_asireserv.reserve_type reserve_type,
       stage_hash_boss_asireserv.start_date start_date,
       stage_hash_boss_asireserv.end_date end_date,
       stage_hash_boss_asireserv.session_id session_id,
       stage_hash_boss_asireserv.program_id program_id,
       stage_hash_boss_asireserv.def_price def_price,
       stage_hash_boss_asireserv.instructor instructor,
       stage_hash_boss_asireserv.billing_count billing_count,
       stage_hash_boss_asireserv.status status,
       stage_hash_boss_asireserv.free_date free_date,
       stage_hash_boss_asireserv.qoh qoh,
       stage_hash_boss_asireserv.limit limit,
       stage_hash_boss_asireserv.recurring recurring,
       stage_hash_boss_asireserv.color color,
       stage_hash_boss_asireserv.shape shape,
       stage_hash_boss_asireserv.comment comment,
       stage_hash_boss_asireserv.resource resource,
       stage_hash_boss_asireserv.create_date create_date,
       stage_hash_boss_asireserv.min_limit min_limit,
       stage_hash_boss_asireserv.non_mbr_price non_mbr_price,
       stage_hash_boss_asireserv.ical_recur_rule ical_recur_rule,
       stage_hash_boss_asireserv.upc_desc upc_desc,
       stage_hash_boss_asireserv.respect_holidays respect_holidays,
       stage_hash_boss_asireserv.origin origin,
       stage_hash_boss_asireserv.print_desc print_desc,
       stage_hash_boss_asireserv.day_plan_string day_plan_string,
       stage_hash_boss_asireserv.day_plan_ints day_plan_ints,
       stage_hash_boss_asireserv.publish publish,
       stage_hash_boss_asireserv.web_register web_register,
       stage_hash_boss_asireserv.target target,
       stage_hash_boss_asireserv.class_expense class_expense,
       stage_hash_boss_asireserv.instructor_expense instructor_expense,
       stage_hash_boss_asireserv.age_low age_low,
       stage_hash_boss_asireserv.age_high age_high,
       stage_hash_boss_asireserv.web_purchase web_purchase,
       stage_hash_boss_asireserv.payment_freq payment_freq,
       stage_hash_boss_asireserv.last_modified last_modified,
       stage_hash_boss_asireserv.waiver_reqd waiver_reqd,
       stage_hash_boss_asireserv.deposit_amt deposit_amt,
       stage_hash_boss_asireserv.payment_reqd_days payment_reqd_days,
       stage_hash_boss_asireserv.pre_assgn_instr_cnt pre_assgn_instr_cnt,
       stage_hash_boss_asireserv.grace_days grace_days,
       stage_hash_boss_asireserv.continuous continuous,
       stage_hash_boss_asireserv.allow_waitlist allow_wait_list,
       stage_hash_boss_asireserv.use_for_LTBucks use_for_LT_Bucks,
       stage_hash_boss_asireserv.cancel_dates cancel_dates,
       stage_hash_boss_asireserv.published_duration published_duration,
       isnull(cast(stage_hash_boss_asireserv.create_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.reservation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.trainer_mbr_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.reserve_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asireserv.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asireserv.end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.session_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.program_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.def_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.instructor,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.billing_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asireserv.free_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.qoh as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.limit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.recurring,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.color as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.shape as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.resource,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asireserv.create_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.min_limit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.non_mbr_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.ical_recur_rule,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.upc_desc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.respect_holidays as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.origin,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.print_desc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.day_plan_string,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.day_plan_ints,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.publish,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.web_register,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.target as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.class_expense as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.instructor_expense as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.age_low as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.age_high as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.web_purchase,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.payment_freq,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asireserv.last_modified,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.waiver_reqd,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.deposit_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.payment_reqd_days as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.pre_assgn_instr_cnt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.grace_days as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.continuous,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.allow_waitlist,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.use_for_LTBucks,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asireserv.cancel_dates,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asireserv.published_duration as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asireserv
 where stage_hash_boss_asireserv.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_reserv records
set @insert_date_time = getdate()
insert into s_boss_asi_reserv (
       bk_hash,
       reservation,
       trainer_mbr_code,
       reserve_type,
       start_date,
       end_date,
       session_id,
       program_id,
       def_price,
       instructor,
       billing_count,
       status,
       free_date,
       qoh,
       limit,
       recurring,
       color,
       shape,
       comment,
       resource,
       create_date,
       min_limit,
       non_mbr_price,
       ical_recur_rule,
       upc_desc,
       respect_holidays,
       origin,
       print_desc,
       day_plan_string,
       day_plan_ints,
       publish,
       web_register,
       target,
       class_expense,
       instructor_expense,
       age_low,
       age_high,
       web_purchase,
       payment_freq,
       last_modified,
       waiver_reqd,
       deposit_amt,
       payment_reqd_days,
       pre_assgn_instr_cnt,
       grace_days,
       continuous,
       allow_wait_list,
       use_for_LT_Bucks,
       cancel_dates,
       published_duration,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_reserv_inserts.bk_hash,
       #s_boss_asi_reserv_inserts.reservation,
       #s_boss_asi_reserv_inserts.trainer_mbr_code,
       #s_boss_asi_reserv_inserts.reserve_type,
       #s_boss_asi_reserv_inserts.start_date,
       #s_boss_asi_reserv_inserts.end_date,
       #s_boss_asi_reserv_inserts.session_id,
       #s_boss_asi_reserv_inserts.program_id,
       #s_boss_asi_reserv_inserts.def_price,
       #s_boss_asi_reserv_inserts.instructor,
       #s_boss_asi_reserv_inserts.billing_count,
       #s_boss_asi_reserv_inserts.status,
       #s_boss_asi_reserv_inserts.free_date,
       #s_boss_asi_reserv_inserts.qoh,
       #s_boss_asi_reserv_inserts.limit,
       #s_boss_asi_reserv_inserts.recurring,
       #s_boss_asi_reserv_inserts.color,
       #s_boss_asi_reserv_inserts.shape,
       #s_boss_asi_reserv_inserts.comment,
       #s_boss_asi_reserv_inserts.resource,
       #s_boss_asi_reserv_inserts.create_date,
       #s_boss_asi_reserv_inserts.min_limit,
       #s_boss_asi_reserv_inserts.non_mbr_price,
       #s_boss_asi_reserv_inserts.ical_recur_rule,
       #s_boss_asi_reserv_inserts.upc_desc,
       #s_boss_asi_reserv_inserts.respect_holidays,
       #s_boss_asi_reserv_inserts.origin,
       #s_boss_asi_reserv_inserts.print_desc,
       #s_boss_asi_reserv_inserts.day_plan_string,
       #s_boss_asi_reserv_inserts.day_plan_ints,
       #s_boss_asi_reserv_inserts.publish,
       #s_boss_asi_reserv_inserts.web_register,
       #s_boss_asi_reserv_inserts.target,
       #s_boss_asi_reserv_inserts.class_expense,
       #s_boss_asi_reserv_inserts.instructor_expense,
       #s_boss_asi_reserv_inserts.age_low,
       #s_boss_asi_reserv_inserts.age_high,
       #s_boss_asi_reserv_inserts.web_purchase,
       #s_boss_asi_reserv_inserts.payment_freq,
       #s_boss_asi_reserv_inserts.last_modified,
       #s_boss_asi_reserv_inserts.waiver_reqd,
       #s_boss_asi_reserv_inserts.deposit_amt,
       #s_boss_asi_reserv_inserts.payment_reqd_days,
       #s_boss_asi_reserv_inserts.pre_assgn_instr_cnt,
       #s_boss_asi_reserv_inserts.grace_days,
       #s_boss_asi_reserv_inserts.continuous,
       #s_boss_asi_reserv_inserts.allow_wait_list,
       #s_boss_asi_reserv_inserts.use_for_LT_Bucks,
       #s_boss_asi_reserv_inserts.cancel_dates,
       #s_boss_asi_reserv_inserts.published_duration,
       case when s_boss_asi_reserv.s_boss_asi_reserv_id is null then isnull(#s_boss_asi_reserv_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_reserv_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_reserv_inserts
  left join p_boss_asi_reserv
    on #s_boss_asi_reserv_inserts.bk_hash = p_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_reserv
    on p_boss_asi_reserv.bk_hash = s_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.s_boss_asi_reserv_id = s_boss_asi_reserv.s_boss_asi_reserv_id
 where s_boss_asi_reserv.s_boss_asi_reserv_id is null
    or (s_boss_asi_reserv.s_boss_asi_reserv_id is not null
        and s_boss_asi_reserv.dv_hash <> #s_boss_asi_reserv_inserts.source_hash)

--Run the dv_deleted proc
exec dbo.proc_dv_deleted_boss_asi_reserv @current_dv_batch_id, @job_start_date_time_varchar

--Run the PIT proc
exec dbo.proc_p_boss_asi_reserv @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_reserv @current_dv_batch_id

end
