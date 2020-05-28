CREATE PROC [dbo].[proc_etl_mms_subsidy_company] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_mms_subsidy_company @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_mms_SubsidyCompany_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(SubsidyCompanyID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_mms_SubsidyCompany
 where (SubsidyCompanyID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_mms_subsidy_company_insert_stage_mms_SubsidyCompany') is not null drop table #h_mms_subsidy_company_insert_stage_mms_SubsidyCompany
create table #h_mms_subsidy_company_insert_stage_mms_SubsidyCompany with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_mms_SubsidyCompany.SubsidyCompanyID subsidy_company_id,
       isnull(stage_mms_SubsidyCompany.InsertedDateTime,'Jan 1, 1980') dv_load_date_time,
       h_mms_subsidy_company.h_mms_subsidy_company_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_SubsidyCompany
  join #incrementals
    on stage_mms_SubsidyCompany.stage_mms_SubsidyCompany_id = #incrementals.source_table_id
   and stage_mms_SubsidyCompany.dv_batch_id = #incrementals.dv_batch_id
  left join h_mms_subsidy_company
    on #incrementals.bk_hash = h_mms_subsidy_company.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_mms_subsidy_company_insert_stage_mms_SubsidyCompany)

while @start <= @end
begin

insert into h_mms_subsidy_company (
       bk_hash,
       subsidy_company_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       subsidy_company_id,
       dv_load_date_time,
       @current_dv_batch_id,
       2,
       getdate(),
       @user
  from #h_mms_subsidy_company_insert_stage_mms_SubsidyCompany
 where h_mms_subsidy_company_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_mms_subsidy_company_current') is not null drop table #p_mms_subsidy_company_current
create table #p_mms_subsidy_company_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_mms_subsidy_company.bk_hash,
       p_mms_subsidy_company.p_mms_subsidy_company_id,
       p_mms_subsidy_company.subsidy_company_id,
       p_mms_subsidy_company.l_mms_subsidy_company_id,
       p_mms_subsidy_company.s_mms_subsidy_company_id,
       p_mms_subsidy_company.dv_load_end_date_time
  from dbo.p_mms_subsidy_company
  join (select distinct bk_hash from #incrementals) inc
    on p_mms_subsidy_company.bk_hash = inc.bk_hash
 where p_mms_subsidy_company.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get l_mms_subsidy_company current hash
if object_id('tempdb..#l_mms_subsidy_company_current') is not null drop table #l_mms_subsidy_company_current
create table #l_mms_subsidy_company_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select l_mms_subsidy_company.l_mms_subsidy_company_id,
       l_mms_subsidy_company.bk_hash,
       l_mms_subsidy_company.dv_hash
  from dbo.l_mms_subsidy_company
  join #p_mms_subsidy_company_current
    on l_mms_subsidy_company.l_mms_subsidy_company_id = #p_mms_subsidy_company_current.l_mms_subsidy_company_id
   and l_mms_subsidy_company.bk_hash = #p_mms_subsidy_company_current.bk_hash

--calculate hash and lookup to current l_mms_subsidy_company
if object_id('tempdb..#l_mms_subsidy_company_inserts') is not null drop table #l_mms_subsidy_company_inserts
create table #l_mms_subsidy_company_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_mms_SubsidyCompany.SubsidyCompanyID subsidy_company_id,
       stage_mms_SubsidyCompany.CompanyID company_id,
       stage_mms_SubsidyCompany.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_SubsidyCompany.SubsidyCompanyID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_SubsidyCompany.CompanyID as varchar(500)),'z#@$k%&P'))),2) source_hash,
       #l_mms_subsidy_company_current.l_mms_subsidy_company_id,
       #l_mms_subsidy_company_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_SubsidyCompany
  join #incrementals
    on stage_mms_SubsidyCompany.stage_mms_SubsidyCompany_id = #incrementals.source_table_id
   and stage_mms_SubsidyCompany.dv_batch_id = #incrementals.dv_batch_id
  left join #l_mms_subsidy_company_current
    on #incrementals.bk_hash = #l_mms_subsidy_company_current.bk_hash

--Insert all updated and new l_mms_subsidy_company records
set @start = 1
set @end = (select max(r) from #l_mms_subsidy_company_inserts)

while @start <= @end
begin

insert into l_mms_subsidy_company (
       bk_hash,
       subsidy_company_id,
       company_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       subsidy_company_id,
       company_id,
       case when l_mms_subsidy_company_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       source_hash,
       getdate(),
       @user
  from #l_mms_subsidy_company_inserts
 where (l_mms_subsidy_company_id is null
        or (l_mms_subsidy_company_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Get s_mms_subsidy_company current hash
if object_id('tempdb..#s_mms_subsidy_company_current') is not null drop table #s_mms_subsidy_company_current
create table #s_mms_subsidy_company_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_mms_subsidy_company.s_mms_subsidy_company_id,
       s_mms_subsidy_company.bk_hash,
       s_mms_subsidy_company.dv_hash
  from dbo.s_mms_subsidy_company
  join #p_mms_subsidy_company_current
    on s_mms_subsidy_company.s_mms_subsidy_company_id = #p_mms_subsidy_company_current.s_mms_subsidy_company_id
   and s_mms_subsidy_company.bk_hash = #p_mms_subsidy_company_current.bk_hash

--calculate hash and lookup to current s_mms_subsidy_company
if object_id('tempdb..#s_mms_subsidy_company_inserts') is not null drop table #s_mms_subsidy_company_inserts
create table #s_mms_subsidy_company_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_mms_SubsidyCompany.SubsidyCompanyID subsidy_company_id,
       stage_mms_SubsidyCompany.Description description,
       stage_mms_SubsidyCompany.LTFEmailDistributionList ltf_email_distribution_list,
       stage_mms_SubsidyCompany.PartnerEmailDistributionList partner_email_distribution_list,
       stage_mms_SubsidyCompany.InsertedDateTime inserted_date_time,
       stage_mms_SubsidyCompany.UpdatedDateTime updated_date_time,
       stage_mms_SubsidyCompany.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_SubsidyCompany.SubsidyCompanyID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_SubsidyCompany.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_SubsidyCompany.LTFEmailDistributionList,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_SubsidyCompany.PartnerEmailDistributionList,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_SubsidyCompany.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_SubsidyCompany.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash,
       #s_mms_subsidy_company_current.s_mms_subsidy_company_id,
       #s_mms_subsidy_company_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_SubsidyCompany
  join #incrementals
    on stage_mms_SubsidyCompany.stage_mms_SubsidyCompany_id = #incrementals.source_table_id
   and stage_mms_SubsidyCompany.dv_batch_id = #incrementals.dv_batch_id
  left join #s_mms_subsidy_company_current
    on #incrementals.bk_hash = #s_mms_subsidy_company_current.bk_hash

--Insert all updated and new s_mms_subsidy_company records
set @start = 1
set @end = (select max(r) from #s_mms_subsidy_company_inserts)

while @start <= @end
begin

insert into s_mms_subsidy_company (
       bk_hash,
       subsidy_company_id,
       description,
       ltf_email_distribution_list,
       partner_email_distribution_list,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       subsidy_company_id,
       description,
       ltf_email_distribution_list,
       partner_email_distribution_list,
       inserted_date_time,
       updated_date_time,
       case when s_mms_subsidy_company_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       source_hash,
       getdate(),
       @user
  from #s_mms_subsidy_company_inserts
 where (s_mms_subsidy_company_id is null
        or (s_mms_subsidy_company_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_mms_subsidy_company @current_dv_batch_id

--Done!
drop table #incrementals
end
