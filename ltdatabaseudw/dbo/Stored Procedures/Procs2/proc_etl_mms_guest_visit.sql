CREATE PROC [dbo].[proc_etl_mms_guest_visit] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_GuestVisit

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_GuestVisit (
       bk_hash,
       GuestVisitID,
       GuestID,
       ClubID,
       VisitDateTime,
       ValGuestAccessMethodID,
       MemberID,
       InsertedDateTime,
       UpdatedDateTime,
       EmployeeID,
       Comment,
       PromotionCode,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(GuestVisitID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       GuestVisitID,
       GuestID,
       ClubID,
       VisitDateTime,
       ValGuestAccessMethodID,
       MemberID,
       InsertedDateTime,
       UpdatedDateTime,
       EmployeeID,
       Comment,
       PromotionCode,
       isnull(cast(stage_mms_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_GuestVisit
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_guest_visit @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_guest_visit (
       bk_hash,
       guest_visit_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_GuestVisit.bk_hash,
       stage_hash_mms_GuestVisit.GuestVisitID guest_visit_id,
       isnull(cast(stage_hash_mms_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_GuestVisit
  left join h_mms_guest_visit
    on stage_hash_mms_GuestVisit.bk_hash = h_mms_guest_visit.bk_hash
 where h_mms_guest_visit_id is null
   and stage_hash_mms_GuestVisit.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_guest_visit
if object_id('tempdb..#l_mms_guest_visit_inserts') is not null drop table #l_mms_guest_visit_inserts
create table #l_mms_guest_visit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GuestVisit.bk_hash,
       stage_hash_mms_GuestVisit.GuestVisitID guest_visit_id,
       stage_hash_mms_GuestVisit.GuestID guest_id,
       stage_hash_mms_GuestVisit.ClubID club_id,
       stage_hash_mms_GuestVisit.ValGuestAccessMethodID val_guest_access_method_id,
       stage_hash_mms_GuestVisit.MemberID member_id,
       stage_hash_mms_GuestVisit.EmployeeID employee_id,
       isnull(cast(stage_hash_mms_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GuestVisit.GuestVisitID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestVisit.GuestID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestVisit.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestVisit.ValGuestAccessMethodID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestVisit.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_GuestVisit.EmployeeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GuestVisit
 where stage_hash_mms_GuestVisit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_guest_visit records
set @insert_date_time = getdate()
insert into l_mms_guest_visit (
       bk_hash,
       guest_visit_id,
       guest_id,
       club_id,
       val_guest_access_method_id,
       member_id,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_guest_visit_inserts.bk_hash,
       #l_mms_guest_visit_inserts.guest_visit_id,
       #l_mms_guest_visit_inserts.guest_id,
       #l_mms_guest_visit_inserts.club_id,
       #l_mms_guest_visit_inserts.val_guest_access_method_id,
       #l_mms_guest_visit_inserts.member_id,
       #l_mms_guest_visit_inserts.employee_id,
       case when l_mms_guest_visit.l_mms_guest_visit_id is null then isnull(#l_mms_guest_visit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_guest_visit_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_guest_visit_inserts
  left join p_mms_guest_visit
    on #l_mms_guest_visit_inserts.bk_hash = p_mms_guest_visit.bk_hash
   and p_mms_guest_visit.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_guest_visit
    on p_mms_guest_visit.bk_hash = l_mms_guest_visit.bk_hash
   and p_mms_guest_visit.l_mms_guest_visit_id = l_mms_guest_visit.l_mms_guest_visit_id
 where l_mms_guest_visit.l_mms_guest_visit_id is null
    or (l_mms_guest_visit.l_mms_guest_visit_id is not null
        and l_mms_guest_visit.dv_hash <> #l_mms_guest_visit_inserts.source_hash)

--calculate hash and lookup to current s_mms_guest_visit
if object_id('tempdb..#s_mms_guest_visit_inserts') is not null drop table #s_mms_guest_visit_inserts
create table #s_mms_guest_visit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_GuestVisit.bk_hash,
       stage_hash_mms_GuestVisit.GuestVisitID guest_visit_id,
       stage_hash_mms_GuestVisit.VisitDateTime visit_date_time,
       stage_hash_mms_GuestVisit.InsertedDateTime inserted_date_time,
       stage_hash_mms_GuestVisit.UpdatedDateTime updated_date_time,
       stage_hash_mms_GuestVisit.Comment comment,
       stage_hash_mms_GuestVisit.PromotionCode promotion_code,
       isnull(cast(stage_hash_mms_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_GuestVisit.GuestVisitID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestVisit.VisitDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestVisit.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_GuestVisit.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_GuestVisit.Comment,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_GuestVisit.PromotionCode,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_GuestVisit
 where stage_hash_mms_GuestVisit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_guest_visit records
set @insert_date_time = getdate()
insert into s_mms_guest_visit (
       bk_hash,
       guest_visit_id,
       visit_date_time,
       inserted_date_time,
       updated_date_time,
       comment,
       promotion_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_guest_visit_inserts.bk_hash,
       #s_mms_guest_visit_inserts.guest_visit_id,
       #s_mms_guest_visit_inserts.visit_date_time,
       #s_mms_guest_visit_inserts.inserted_date_time,
       #s_mms_guest_visit_inserts.updated_date_time,
       #s_mms_guest_visit_inserts.comment,
       #s_mms_guest_visit_inserts.promotion_code,
       case when s_mms_guest_visit.s_mms_guest_visit_id is null then isnull(#s_mms_guest_visit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_guest_visit_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_guest_visit_inserts
  left join p_mms_guest_visit
    on #s_mms_guest_visit_inserts.bk_hash = p_mms_guest_visit.bk_hash
   and p_mms_guest_visit.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_guest_visit
    on p_mms_guest_visit.bk_hash = s_mms_guest_visit.bk_hash
   and p_mms_guest_visit.s_mms_guest_visit_id = s_mms_guest_visit.s_mms_guest_visit_id
 where s_mms_guest_visit.s_mms_guest_visit_id is null
    or (s_mms_guest_visit.s_mms_guest_visit_id is not null
        and s_mms_guest_visit.dv_hash <> #s_mms_guest_visit_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_guest_visit @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_guest_visit @current_dv_batch_id

end
