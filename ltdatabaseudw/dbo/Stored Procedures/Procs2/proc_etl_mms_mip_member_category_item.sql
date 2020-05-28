CREATE PROC [dbo].[proc_etl_mms_mip_member_category_item] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_mms_mip_member_category_item @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_mms_MIPMemberCategoryItem_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MIPMemberCategoryItemID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_mms_MIPMemberCategoryItem
 where (MIPMemberCategoryItemID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_mms_mip_member_category_item_insert_stage_mms_MIPMemberCategoryItem') is not null drop table #h_mms_mip_member_category_item_insert_stage_mms_MIPMemberCategoryItem
create table #h_mms_mip_member_category_item_insert_stage_mms_MIPMemberCategoryItem with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_mms_MIPMemberCategoryItem.MIPMemberCategoryItemID mip_member_category_item_id,
       isnull(stage_mms_MIPMemberCategoryItem.InsertedDateTime,'Jan 1, 1980') dv_load_date_time,
       h_mms_mip_member_category_item.h_mms_mip_member_category_item_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_MIPMemberCategoryItem
  join #incrementals
    on stage_mms_MIPMemberCategoryItem.stage_mms_MIPMemberCategoryItem_id = #incrementals.source_table_id
   and stage_mms_MIPMemberCategoryItem.dv_batch_id = #incrementals.dv_batch_id
  left join h_mms_mip_member_category_item
    on #incrementals.bk_hash = h_mms_mip_member_category_item.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_mms_mip_member_category_item_insert_stage_mms_MIPMemberCategoryItem)

while @start <= @end
begin

insert into h_mms_mip_member_category_item (
       bk_hash,
       mip_member_category_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       mip_member_category_item_id,
       dv_load_date_time,
       @current_dv_batch_id,
       2,
       getdate(),
       @user
  from #h_mms_mip_member_category_item_insert_stage_mms_MIPMemberCategoryItem
 where h_mms_mip_member_category_item_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_mms_mip_member_category_item_current') is not null drop table #p_mms_mip_member_category_item_current
create table #p_mms_mip_member_category_item_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_mms_mip_member_category_item.bk_hash,
       p_mms_mip_member_category_item.p_mms_mip_member_category_item_id,
       p_mms_mip_member_category_item.mip_member_category_item_id,
       p_mms_mip_member_category_item.l_mms_mip_member_category_item_id,
       p_mms_mip_member_category_item.s_mms_mip_member_category_item_id,
       p_mms_mip_member_category_item.dv_load_end_date_time
  from dbo.p_mms_mip_member_category_item
  join (select distinct bk_hash from #incrementals) inc
    on p_mms_mip_member_category_item.bk_hash = inc.bk_hash
 where p_mms_mip_member_category_item.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get l_mms_mip_member_category_item current hash
if object_id('tempdb..#l_mms_mip_member_category_item_current') is not null drop table #l_mms_mip_member_category_item_current
create table #l_mms_mip_member_category_item_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select l_mms_mip_member_category_item.l_mms_mip_member_category_item_id,
       l_mms_mip_member_category_item.bk_hash,
       l_mms_mip_member_category_item.dv_hash
  from dbo.l_mms_mip_member_category_item
  join #p_mms_mip_member_category_item_current
    on l_mms_mip_member_category_item.l_mms_mip_member_category_item_id = #p_mms_mip_member_category_item_current.l_mms_mip_member_category_item_id
   and l_mms_mip_member_category_item.bk_hash = #p_mms_mip_member_category_item_current.bk_hash

--calculate hash and lookup to current l_mms_mip_member_category_item
if object_id('tempdb..#l_mms_mip_member_category_item_inserts') is not null drop table #l_mms_mip_member_category_item_inserts
create table #l_mms_mip_member_category_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_mms_MIPMemberCategoryItem.MIPMemberCategoryItemID mip_member_category_item_id,
       stage_mms_MIPMemberCategoryItem.MemberID member_id,
       stage_mms_MIPMemberCategoryItem.MIPCategoryItemID mip_category_item_id,
       stage_mms_MIPMemberCategoryItem.ClubID club_id,
       stage_mms_MIPMemberCategoryItem.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_MIPMemberCategoryItem.MIPMemberCategoryItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_MIPMemberCategoryItem.MemberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_MIPMemberCategoryItem.MIPCategoryItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_MIPMemberCategoryItem.ClubID as varchar(500)),'z#@$k%&P'))),2) source_hash,
       #l_mms_mip_member_category_item_current.l_mms_mip_member_category_item_id,
       #l_mms_mip_member_category_item_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_MIPMemberCategoryItem
  join #incrementals
    on stage_mms_MIPMemberCategoryItem.stage_mms_MIPMemberCategoryItem_id = #incrementals.source_table_id
   and stage_mms_MIPMemberCategoryItem.dv_batch_id = #incrementals.dv_batch_id
  left join #l_mms_mip_member_category_item_current
    on #incrementals.bk_hash = #l_mms_mip_member_category_item_current.bk_hash

--Insert all updated and new l_mms_mip_member_category_item records
set @start = 1
set @end = (select max(r) from #l_mms_mip_member_category_item_inserts)

while @start <= @end
begin

insert into l_mms_mip_member_category_item (
       bk_hash,
       mip_member_category_item_id,
       member_id,
       mip_category_item_id,
       club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       mip_member_category_item_id,
       member_id,
       mip_category_item_id,
       club_id,
       case when l_mms_mip_member_category_item_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       source_hash,
       getdate(),
       @user
  from #l_mms_mip_member_category_item_inserts
 where (l_mms_mip_member_category_item_id is null
        or (l_mms_mip_member_category_item_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Get s_mms_mip_member_category_item current hash
if object_id('tempdb..#s_mms_mip_member_category_item_current') is not null drop table #s_mms_mip_member_category_item_current
create table #s_mms_mip_member_category_item_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_mms_mip_member_category_item.s_mms_mip_member_category_item_id,
       s_mms_mip_member_category_item.bk_hash,
       s_mms_mip_member_category_item.dv_hash
  from dbo.s_mms_mip_member_category_item
  join #p_mms_mip_member_category_item_current
    on s_mms_mip_member_category_item.s_mms_mip_member_category_item_id = #p_mms_mip_member_category_item_current.s_mms_mip_member_category_item_id
   and s_mms_mip_member_category_item.bk_hash = #p_mms_mip_member_category_item_current.bk_hash

--calculate hash and lookup to current s_mms_mip_member_category_item
if object_id('tempdb..#s_mms_mip_member_category_item_inserts') is not null drop table #s_mms_mip_member_category_item_inserts
create table #s_mms_mip_member_category_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_mms_MIPMemberCategoryItem.MIPMemberCategoryItemID mip_member_category_item_id,
       stage_mms_MIPMemberCategoryItem.InsertedDateTime inserted_date_time,
       stage_mms_MIPMemberCategoryItem.UpdatedDateTime updated_date_time,
       stage_mms_MIPMemberCategoryItem.EmailFlag email_flag,
       stage_mms_MIPMemberCategoryItem.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_MIPMemberCategoryItem.MIPMemberCategoryItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_MIPMemberCategoryItem.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_MIPMemberCategoryItem.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_MIPMemberCategoryItem.EmailFlag as varchar(42)),'z#@$k%&P'))),2) source_hash,
       #s_mms_mip_member_category_item_current.s_mms_mip_member_category_item_id,
       #s_mms_mip_member_category_item_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_MIPMemberCategoryItem
  join #incrementals
    on stage_mms_MIPMemberCategoryItem.stage_mms_MIPMemberCategoryItem_id = #incrementals.source_table_id
   and stage_mms_MIPMemberCategoryItem.dv_batch_id = #incrementals.dv_batch_id
  left join #s_mms_mip_member_category_item_current
    on #incrementals.bk_hash = #s_mms_mip_member_category_item_current.bk_hash

--Insert all updated and new s_mms_mip_member_category_item records
set @start = 1
set @end = (select max(r) from #s_mms_mip_member_category_item_inserts)

while @start <= @end
begin

insert into s_mms_mip_member_category_item (
       bk_hash,
       mip_member_category_item_id,
       inserted_date_time,
       updated_date_time,
       email_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       mip_member_category_item_id,
       inserted_date_time,
       updated_date_time,
       email_flag,
       case when s_mms_mip_member_category_item_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       source_hash,
       getdate(),
       @user
  from #s_mms_mip_member_category_item_inserts
 where (s_mms_mip_member_category_item_id is null
        or (s_mms_mip_member_category_item_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_mms_mip_member_category_item @current_dv_batch_id

--Done!
drop table #incrementals
end
