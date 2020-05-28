CREATE PROC [dbo].[proc_etl_boss_asi_player] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asiplayer

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asiplayer (
       bk_hash,
       reservation,
       date_used,
       cust_code,
       mbr_code,
       sequence,
       price,
       tax_amt,
       paid,
       trans,
       instructor,
       comm_paid,
       employee_id,
       phone,
       player_name,
       can_charge,
       checked_in,
       email,
       cancel_date,
       notes,
       status,
       start_date,
       origin,
       DOB,
       mbr_type,
       house_acct,
       created_at,
       [id],
       balance_due,
       contact_id,
       mbrship_type_ID,
       rostered_by,
       cust_type,
       mms_trans_id,
       updated_at,
       pmt_start,
       pmt_end,
       recurrence_id,
       check_in_date,
       last_paid_date,
       mms_swipe,
       package_balance,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       reservation,
       date_used,
       cust_code,
       mbr_code,
       sequence,
       price,
       tax_amt,
       paid,
       trans,
       instructor,
       comm_paid,
       employee_id,
       phone,
       player_name,
       can_charge,
       checked_in,
       email,
       cancel_date,
       notes,
       status,
       start_date,
       origin,
       DOB,
       mbr_type,
       house_acct,
       created_at,
       [id],
       balance_due,
       contact_id,
       mbrship_type_ID,
       rostered_by,
       cust_type,
       mms_trans_id,
       updated_at,
       pmt_start,
       pmt_end,
       recurrence_id,
       check_in_date,
       last_paid_date,
       mms_swipe,
       package_balance,
       isnull(cast(stage_boss_asiplayer.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_asiplayer
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_player @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_player (
       bk_hash,
       asi_player_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asiplayer.bk_hash,
       stage_hash_boss_asiplayer.[id] asi_player_id,
       isnull(cast(stage_hash_boss_asiplayer.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asiplayer
  left join h_boss_asi_player
    on stage_hash_boss_asiplayer.bk_hash = h_boss_asi_player.bk_hash
 where h_boss_asi_player_id is null
   and stage_hash_boss_asiplayer.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_player
if object_id('tempdb..#l_boss_asi_player_inserts') is not null drop table #l_boss_asi_player_inserts
create table #l_boss_asi_player_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiplayer.bk_hash,
       stage_hash_boss_asiplayer.reservation reservation,
       stage_hash_boss_asiplayer.mbr_code mbr_code,
       stage_hash_boss_asiplayer.employee_id employee_id,
       stage_hash_boss_asiplayer.[id] asi_player_id,
       stage_hash_boss_asiplayer.contact_id contact_id,
       stage_hash_boss_asiplayer.mbrship_type_ID mbrship_type_id,
       stage_hash_boss_asiplayer.mms_trans_id mms_trans_id,
       stage_hash_boss_asiplayer.recurrence_id recurrence_id,
       isnull(cast(stage_hash_boss_asiplayer.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.reservation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.mbr_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.contact_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.mbrship_type_ID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.mms_trans_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.recurrence_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiplayer
 where stage_hash_boss_asiplayer.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_player records
set @insert_date_time = getdate()
insert into l_boss_asi_player (
       bk_hash,
       reservation,
       mbr_code,
       employee_id,
       asi_player_id,
       contact_id,
       mbrship_type_id,
       mms_trans_id,
       recurrence_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_player_inserts.bk_hash,
       #l_boss_asi_player_inserts.reservation,
       #l_boss_asi_player_inserts.mbr_code,
       #l_boss_asi_player_inserts.employee_id,
       #l_boss_asi_player_inserts.asi_player_id,
       #l_boss_asi_player_inserts.contact_id,
       #l_boss_asi_player_inserts.mbrship_type_id,
       #l_boss_asi_player_inserts.mms_trans_id,
       #l_boss_asi_player_inserts.recurrence_id,
       case when l_boss_asi_player.l_boss_asi_player_id is null then isnull(#l_boss_asi_player_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_player_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_player_inserts
  left join p_boss_asi_player
    on #l_boss_asi_player_inserts.bk_hash = p_boss_asi_player.bk_hash
   and p_boss_asi_player.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_player
    on p_boss_asi_player.bk_hash = l_boss_asi_player.bk_hash
   and p_boss_asi_player.l_boss_asi_player_id = l_boss_asi_player.l_boss_asi_player_id
 where l_boss_asi_player.l_boss_asi_player_id is null
    or (l_boss_asi_player.l_boss_asi_player_id is not null
        and l_boss_asi_player.dv_hash <> #l_boss_asi_player_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_player
if object_id('tempdb..#s_boss_asi_player_inserts') is not null drop table #s_boss_asi_player_inserts
create table #s_boss_asi_player_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiplayer.bk_hash,
       stage_hash_boss_asiplayer.date_used date_used,
       stage_hash_boss_asiplayer.cust_code cust_code,
       stage_hash_boss_asiplayer.sequence sequence,
       stage_hash_boss_asiplayer.price price,
       stage_hash_boss_asiplayer.tax_amt tax_amt,
       stage_hash_boss_asiplayer.paid paid,
       stage_hash_boss_asiplayer.trans trans,
       stage_hash_boss_asiplayer.instructor instructor,
       stage_hash_boss_asiplayer.comm_paid comm_paid,
       stage_hash_boss_asiplayer.phone phone,
       stage_hash_boss_asiplayer.player_name player_name,
       stage_hash_boss_asiplayer.can_charge can_charge,
       stage_hash_boss_asiplayer.checked_in checked_in,
       stage_hash_boss_asiplayer.email email,
       stage_hash_boss_asiplayer.cancel_date cancel_date,
       stage_hash_boss_asiplayer.notes notes,
       stage_hash_boss_asiplayer.status status,
       stage_hash_boss_asiplayer.start_date start_date,
       stage_hash_boss_asiplayer.origin origin,
       stage_hash_boss_asiplayer.DOB dob,
       stage_hash_boss_asiplayer.mbr_type mbr_type,
       stage_hash_boss_asiplayer.house_acct house_acct,
       stage_hash_boss_asiplayer.created_at created_at,
       stage_hash_boss_asiplayer.[id] asi_player_id,
       stage_hash_boss_asiplayer.balance_due balance_due,
       stage_hash_boss_asiplayer.rostered_by rostered_by,
       stage_hash_boss_asiplayer.cust_type cust_type,
       stage_hash_boss_asiplayer.updated_at updated_at,
       stage_hash_boss_asiplayer.pmt_start pmt_start,
       stage_hash_boss_asiplayer.pmt_end pmt_end,
       stage_hash_boss_asiplayer.check_in_date check_in_date,
       stage_hash_boss_asiplayer.last_paid_date last_paid_date,
       stage_hash_boss_asiplayer.mms_swipe mms_swipe,
       stage_hash_boss_asiplayer.package_balance package_balance,
       isnull(cast(stage_hash_boss_asiplayer.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.date_used,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.cust_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.sequence as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.tax_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.paid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.trans as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.instructor,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.comm_paid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.player_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.can_charge,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.checked_in,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.cancel_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.origin,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.DOB,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.mbr_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.house_acct,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.balance_due as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.rostered_by as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.cust_type as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.pmt_start,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.pmt_end,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.check_in_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiplayer.last_paid_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiplayer.mms_swipe,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiplayer.package_balance as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiplayer
 where stage_hash_boss_asiplayer.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_player records
set @insert_date_time = getdate()
insert into s_boss_asi_player (
       bk_hash,
       date_used,
       cust_code,
       sequence,
       price,
       tax_amt,
       paid,
       trans,
       instructor,
       comm_paid,
       phone,
       player_name,
       can_charge,
       checked_in,
       email,
       cancel_date,
       notes,
       status,
       start_date,
       origin,
       dob,
       mbr_type,
       house_acct,
       created_at,
       asi_player_id,
       balance_due,
       rostered_by,
       cust_type,
       updated_at,
       pmt_start,
       pmt_end,
       check_in_date,
       last_paid_date,
       mms_swipe,
       package_balance,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_player_inserts.bk_hash,
       #s_boss_asi_player_inserts.date_used,
       #s_boss_asi_player_inserts.cust_code,
       #s_boss_asi_player_inserts.sequence,
       #s_boss_asi_player_inserts.price,
       #s_boss_asi_player_inserts.tax_amt,
       #s_boss_asi_player_inserts.paid,
       #s_boss_asi_player_inserts.trans,
       #s_boss_asi_player_inserts.instructor,
       #s_boss_asi_player_inserts.comm_paid,
       #s_boss_asi_player_inserts.phone,
       #s_boss_asi_player_inserts.player_name,
       #s_boss_asi_player_inserts.can_charge,
       #s_boss_asi_player_inserts.checked_in,
       #s_boss_asi_player_inserts.email,
       #s_boss_asi_player_inserts.cancel_date,
       #s_boss_asi_player_inserts.notes,
       #s_boss_asi_player_inserts.status,
       #s_boss_asi_player_inserts.start_date,
       #s_boss_asi_player_inserts.origin,
       #s_boss_asi_player_inserts.dob,
       #s_boss_asi_player_inserts.mbr_type,
       #s_boss_asi_player_inserts.house_acct,
       #s_boss_asi_player_inserts.created_at,
       #s_boss_asi_player_inserts.asi_player_id,
       #s_boss_asi_player_inserts.balance_due,
       #s_boss_asi_player_inserts.rostered_by,
       #s_boss_asi_player_inserts.cust_type,
       #s_boss_asi_player_inserts.updated_at,
       #s_boss_asi_player_inserts.pmt_start,
       #s_boss_asi_player_inserts.pmt_end,
       #s_boss_asi_player_inserts.check_in_date,
       #s_boss_asi_player_inserts.last_paid_date,
       #s_boss_asi_player_inserts.mms_swipe,
       #s_boss_asi_player_inserts.package_balance,
       case when s_boss_asi_player.s_boss_asi_player_id is null then isnull(#s_boss_asi_player_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_player_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_player_inserts
  left join p_boss_asi_player
    on #s_boss_asi_player_inserts.bk_hash = p_boss_asi_player.bk_hash
   and p_boss_asi_player.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_player
    on p_boss_asi_player.bk_hash = s_boss_asi_player.bk_hash
   and p_boss_asi_player.s_boss_asi_player_id = s_boss_asi_player.s_boss_asi_player_id
 where s_boss_asi_player.s_boss_asi_player_id is null
    or (s_boss_asi_player.s_boss_asi_player_id is not null
        and s_boss_asi_player.dv_hash <> #s_boss_asi_player_inserts.source_hash)

--Run the dv_deleted proc
exec dbo.proc_dv_deleted_boss_asi_player @current_dv_batch_id, @job_start_date_time_varchar

--Run the PIT proc
exec dbo.proc_p_boss_asi_player @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_player @current_dv_batch_id

end
