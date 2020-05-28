CREATE PROC [dbo].[proc_etl_ig_ig_dimension_business_period_dimension] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_ig_dimension_Business_Period_Dimension

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_ig_dimension_Business_Period_Dimension (
       bk_hash,
       business_period_dim_id,
       customer_id,
       ent_id,
       business_period_group_id,
       start_date_time,
       end_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(business_period_dim_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       business_period_dim_id,
       customer_id,
       ent_id,
       business_period_group_id,
       start_date_time,
       end_date_time,
       isnull(cast(stage_ig_ig_dimension_Business_Period_Dimension.start_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_ig_dimension_Business_Period_Dimension
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_ig_dimension_business_period_dimension @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_ig_dimension_business_period_dimension (
       bk_hash,
       business_period_dim_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_ig_dimension_Business_Period_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.business_period_dim_id business_period_dim_id,
       isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.start_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       13,
       @insert_date_time,
       @user
  from stage_hash_ig_ig_dimension_Business_Period_Dimension
  left join h_ig_ig_dimension_business_period_dimension
    on stage_hash_ig_ig_dimension_Business_Period_Dimension.bk_hash = h_ig_ig_dimension_business_period_dimension.bk_hash
 where h_ig_ig_dimension_business_period_dimension_id is null
   and stage_hash_ig_ig_dimension_Business_Period_Dimension.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_ig_dimension_business_period_dimension
if object_id('tempdb..#l_ig_ig_dimension_business_period_dimension_inserts') is not null drop table #l_ig_ig_dimension_business_period_dimension_inserts
create table #l_ig_ig_dimension_business_period_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Business_Period_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.business_period_dim_id business_period_dim_id,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.customer_id customer_id,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.ent_id ent_id,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.business_period_group_id business_period_group_id,
       isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.start_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.business_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.customer_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.business_period_group_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Business_Period_Dimension
 where stage_hash_ig_ig_dimension_Business_Period_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_ig_dimension_business_period_dimension records
set @insert_date_time = getdate()
insert into l_ig_ig_dimension_business_period_dimension (
       bk_hash,
       business_period_dim_id,
       customer_id,
       ent_id,
       business_period_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_ig_dimension_business_period_dimension_inserts.bk_hash,
       #l_ig_ig_dimension_business_period_dimension_inserts.business_period_dim_id,
       #l_ig_ig_dimension_business_period_dimension_inserts.customer_id,
       #l_ig_ig_dimension_business_period_dimension_inserts.ent_id,
       #l_ig_ig_dimension_business_period_dimension_inserts.business_period_group_id,
       case when l_ig_ig_dimension_business_period_dimension.l_ig_ig_dimension_business_period_dimension_id is null then isnull(#l_ig_ig_dimension_business_period_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #l_ig_ig_dimension_business_period_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_ig_dimension_business_period_dimension_inserts
  left join p_ig_ig_dimension_business_period_dimension
    on #l_ig_ig_dimension_business_period_dimension_inserts.bk_hash = p_ig_ig_dimension_business_period_dimension.bk_hash
   and p_ig_ig_dimension_business_period_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_ig_dimension_business_period_dimension
    on p_ig_ig_dimension_business_period_dimension.bk_hash = l_ig_ig_dimension_business_period_dimension.bk_hash
   and p_ig_ig_dimension_business_period_dimension.l_ig_ig_dimension_business_period_dimension_id = l_ig_ig_dimension_business_period_dimension.l_ig_ig_dimension_business_period_dimension_id
 where l_ig_ig_dimension_business_period_dimension.l_ig_ig_dimension_business_period_dimension_id is null
    or (l_ig_ig_dimension_business_period_dimension.l_ig_ig_dimension_business_period_dimension_id is not null
        and l_ig_ig_dimension_business_period_dimension.dv_hash <> #l_ig_ig_dimension_business_period_dimension_inserts.source_hash)

--calculate hash and lookup to current s_ig_ig_dimension_business_period_dimension
if object_id('tempdb..#s_ig_ig_dimension_business_period_dimension_inserts') is not null drop table #s_ig_ig_dimension_business_period_dimension_inserts
create table #s_ig_ig_dimension_business_period_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Business_Period_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.business_period_dim_id business_period_dim_id,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.start_date_time start_date_time,
       stage_hash_ig_ig_dimension_Business_Period_Dimension.end_date_time end_date_time,
       isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.start_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Business_Period_Dimension.business_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Business_Period_Dimension.start_date_time,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Business_Period_Dimension.end_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Business_Period_Dimension
 where stage_hash_ig_ig_dimension_Business_Period_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_ig_dimension_business_period_dimension records
set @insert_date_time = getdate()
insert into s_ig_ig_dimension_business_period_dimension (
       bk_hash,
       business_period_dim_id,
       start_date_time,
       end_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_ig_dimension_business_period_dimension_inserts.bk_hash,
       #s_ig_ig_dimension_business_period_dimension_inserts.business_period_dim_id,
       #s_ig_ig_dimension_business_period_dimension_inserts.start_date_time,
       #s_ig_ig_dimension_business_period_dimension_inserts.end_date_time,
       case when s_ig_ig_dimension_business_period_dimension.s_ig_ig_dimension_business_period_dimension_id is null then isnull(#s_ig_ig_dimension_business_period_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #s_ig_ig_dimension_business_period_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_ig_dimension_business_period_dimension_inserts
  left join p_ig_ig_dimension_business_period_dimension
    on #s_ig_ig_dimension_business_period_dimension_inserts.bk_hash = p_ig_ig_dimension_business_period_dimension.bk_hash
   and p_ig_ig_dimension_business_period_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_ig_dimension_business_period_dimension
    on p_ig_ig_dimension_business_period_dimension.bk_hash = s_ig_ig_dimension_business_period_dimension.bk_hash
   and p_ig_ig_dimension_business_period_dimension.s_ig_ig_dimension_business_period_dimension_id = s_ig_ig_dimension_business_period_dimension.s_ig_ig_dimension_business_period_dimension_id
 where s_ig_ig_dimension_business_period_dimension.s_ig_ig_dimension_business_period_dimension_id is null
    or (s_ig_ig_dimension_business_period_dimension.s_ig_ig_dimension_business_period_dimension_id is not null
        and s_ig_ig_dimension_business_period_dimension.dv_hash <> #s_ig_ig_dimension_business_period_dimension_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_ig_dimension_business_period_dimension @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_ig_dimension_business_period_dimension @current_dv_batch_id

end
