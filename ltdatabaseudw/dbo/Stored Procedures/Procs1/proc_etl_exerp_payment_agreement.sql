CREATE PROC [dbo].[proc_etl_exerp_payment_agreement] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_payment_agreement

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_payment_agreement (
       bk_hash,
       id,
       person_id,
       clearinghouse,
       refno,
       state,
       individual_deduction_day,
       expire_date,
       active,
       center_id,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       person_id,
       clearinghouse,
       refno,
       state,
       individual_deduction_day,
       expire_date,
       active,
       center_id,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_payment_agreement.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_payment_agreement
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_payment_agreement @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_payment_agreement (
       bk_hash,
       payment_agreement_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_payment_agreement.bk_hash,
       stage_hash_exerp_payment_agreement.id payment_agreement_id,
       isnull(cast(stage_hash_exerp_payment_agreement.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_payment_agreement
  left join h_exerp_payment_agreement
    on stage_hash_exerp_payment_agreement.bk_hash = h_exerp_payment_agreement.bk_hash
 where h_exerp_payment_agreement_id is null
   and stage_hash_exerp_payment_agreement.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_payment_agreement
if object_id('tempdb..#l_exerp_payment_agreement_inserts') is not null drop table #l_exerp_payment_agreement_inserts
create table #l_exerp_payment_agreement_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_payment_agreement.bk_hash,
       stage_hash_exerp_payment_agreement.id payment_agreement_id,
       stage_hash_exerp_payment_agreement.person_id person_id,
       stage_hash_exerp_payment_agreement.center_id center_id,
       isnull(cast(stage_hash_exerp_payment_agreement.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_payment_agreement.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_payment_agreement.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_payment_agreement.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_payment_agreement
 where stage_hash_exerp_payment_agreement.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_payment_agreement records
set @insert_date_time = getdate()
insert into l_exerp_payment_agreement (
       bk_hash,
       payment_agreement_id,
       person_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_payment_agreement_inserts.bk_hash,
       #l_exerp_payment_agreement_inserts.payment_agreement_id,
       #l_exerp_payment_agreement_inserts.person_id,
       #l_exerp_payment_agreement_inserts.center_id,
       case when l_exerp_payment_agreement.l_exerp_payment_agreement_id is null then isnull(#l_exerp_payment_agreement_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_payment_agreement_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_payment_agreement_inserts
  left join p_exerp_payment_agreement
    on #l_exerp_payment_agreement_inserts.bk_hash = p_exerp_payment_agreement.bk_hash
   and p_exerp_payment_agreement.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_payment_agreement
    on p_exerp_payment_agreement.bk_hash = l_exerp_payment_agreement.bk_hash
   and p_exerp_payment_agreement.l_exerp_payment_agreement_id = l_exerp_payment_agreement.l_exerp_payment_agreement_id
 where l_exerp_payment_agreement.l_exerp_payment_agreement_id is null
    or (l_exerp_payment_agreement.l_exerp_payment_agreement_id is not null
        and l_exerp_payment_agreement.dv_hash <> #l_exerp_payment_agreement_inserts.source_hash)

--calculate hash and lookup to current s_exerp_payment_agreement
if object_id('tempdb..#s_exerp_payment_agreement_inserts') is not null drop table #s_exerp_payment_agreement_inserts
create table #s_exerp_payment_agreement_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_payment_agreement.bk_hash,
       stage_hash_exerp_payment_agreement.id payment_agreement_id,
       stage_hash_exerp_payment_agreement.clearinghouse clearing_house,
       stage_hash_exerp_payment_agreement.refno ref_no,
       stage_hash_exerp_payment_agreement.state state,
       stage_hash_exerp_payment_agreement.individual_deduction_day individual_deduction_day,
       stage_hash_exerp_payment_agreement.expire_date expire_date,
       stage_hash_exerp_payment_agreement.active active,
       stage_hash_exerp_payment_agreement.ets ets,
       stage_hash_exerp_payment_agreement.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_payment_agreement.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_payment_agreement.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_payment_agreement.clearinghouse,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_payment_agreement.refno,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_payment_agreement.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_payment_agreement.individual_deduction_day as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_payment_agreement.expire_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_payment_agreement.active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_payment_agreement.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_payment_agreement
 where stage_hash_exerp_payment_agreement.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_payment_agreement records
set @insert_date_time = getdate()
insert into s_exerp_payment_agreement (
       bk_hash,
       payment_agreement_id,
       clearing_house,
       ref_no,
       state,
       individual_deduction_day,
       expire_date,
       active,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_payment_agreement_inserts.bk_hash,
       #s_exerp_payment_agreement_inserts.payment_agreement_id,
       #s_exerp_payment_agreement_inserts.clearing_house,
       #s_exerp_payment_agreement_inserts.ref_no,
       #s_exerp_payment_agreement_inserts.state,
       #s_exerp_payment_agreement_inserts.individual_deduction_day,
       #s_exerp_payment_agreement_inserts.expire_date,
       #s_exerp_payment_agreement_inserts.active,
       #s_exerp_payment_agreement_inserts.ets,
       #s_exerp_payment_agreement_inserts.dummy_modified_date_time,
       case when s_exerp_payment_agreement.s_exerp_payment_agreement_id is null then isnull(#s_exerp_payment_agreement_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_payment_agreement_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_payment_agreement_inserts
  left join p_exerp_payment_agreement
    on #s_exerp_payment_agreement_inserts.bk_hash = p_exerp_payment_agreement.bk_hash
   and p_exerp_payment_agreement.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_payment_agreement
    on p_exerp_payment_agreement.bk_hash = s_exerp_payment_agreement.bk_hash
   and p_exerp_payment_agreement.s_exerp_payment_agreement_id = s_exerp_payment_agreement.s_exerp_payment_agreement_id
 where s_exerp_payment_agreement.s_exerp_payment_agreement_id is null
    or (s_exerp_payment_agreement.s_exerp_payment_agreement_id is not null
        and s_exerp_payment_agreement.dv_hash <> #s_exerp_payment_agreement_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_payment_agreement @current_dv_batch_id

end
