CREATE PROC [dbo].[proc_etl_exerp_subscription_price] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_subscription_price

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_subscription_price (
       bk_hash,
       id,
       subscription_id,
       type,
       entry_datetime,
       from_date,
       to_date,
       price,
       cancelled,
       cancel_datetime,
       center_id,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       subscription_id,
       type,
       entry_datetime,
       from_date,
       to_date,
       price,
       cancelled,
       cancel_datetime,
       center_id,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_subscription_price.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_subscription_price
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_subscription_price @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_subscription_price (
       bk_hash,
       subscription_price_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_subscription_price.bk_hash,
       stage_hash_exerp_subscription_price.id subscription_price_id,
       isnull(cast(stage_hash_exerp_subscription_price.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_subscription_price
  left join h_exerp_subscription_price
    on stage_hash_exerp_subscription_price.bk_hash = h_exerp_subscription_price.bk_hash
 where h_exerp_subscription_price_id is null
   and stage_hash_exerp_subscription_price.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_subscription_price
if object_id('tempdb..#l_exerp_subscription_price_inserts') is not null drop table #l_exerp_subscription_price_inserts
create table #l_exerp_subscription_price_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_subscription_price.bk_hash,
       stage_hash_exerp_subscription_price.id subscription_price_id,
       stage_hash_exerp_subscription_price.subscription_id subscription_id,
       stage_hash_exerp_subscription_price.center_id center_id,
       isnull(cast(stage_hash_exerp_subscription_price.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_price.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_price.subscription_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_price.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_subscription_price
 where stage_hash_exerp_subscription_price.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_subscription_price records
set @insert_date_time = getdate()
insert into l_exerp_subscription_price (
       bk_hash,
       subscription_price_id,
       subscription_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_subscription_price_inserts.bk_hash,
       #l_exerp_subscription_price_inserts.subscription_price_id,
       #l_exerp_subscription_price_inserts.subscription_id,
       #l_exerp_subscription_price_inserts.center_id,
       case when l_exerp_subscription_price.l_exerp_subscription_price_id is null then isnull(#l_exerp_subscription_price_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_subscription_price_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_subscription_price_inserts
  left join p_exerp_subscription_price
    on #l_exerp_subscription_price_inserts.bk_hash = p_exerp_subscription_price.bk_hash
   and p_exerp_subscription_price.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_subscription_price
    on p_exerp_subscription_price.bk_hash = l_exerp_subscription_price.bk_hash
   and p_exerp_subscription_price.l_exerp_subscription_price_id = l_exerp_subscription_price.l_exerp_subscription_price_id
 where l_exerp_subscription_price.l_exerp_subscription_price_id is null
    or (l_exerp_subscription_price.l_exerp_subscription_price_id is not null
        and l_exerp_subscription_price.dv_hash <> #l_exerp_subscription_price_inserts.source_hash)

--calculate hash and lookup to current s_exerp_subscription_price
if object_id('tempdb..#s_exerp_subscription_price_inserts') is not null drop table #s_exerp_subscription_price_inserts
create table #s_exerp_subscription_price_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_subscription_price.bk_hash,
       stage_hash_exerp_subscription_price.id subscription_price_id,
       stage_hash_exerp_subscription_price.type type,
       stage_hash_exerp_subscription_price.entry_datetime entry_datetime,
       stage_hash_exerp_subscription_price.from_date from_date,
       stage_hash_exerp_subscription_price.to_date to_date,
       stage_hash_exerp_subscription_price.price price,
       stage_hash_exerp_subscription_price.cancelled cancelled,
       stage_hash_exerp_subscription_price.cancel_datetime cancel_datetime,
       stage_hash_exerp_subscription_price.ets ets,
       stage_hash_exerp_subscription_price.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_subscription_price.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_price.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_subscription_price.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription_price.entry_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription_price.from_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription_price.to_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_price.price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_price.cancelled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_subscription_price.cancel_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_subscription_price.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_subscription_price
 where stage_hash_exerp_subscription_price.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_subscription_price records
set @insert_date_time = getdate()
insert into s_exerp_subscription_price (
       bk_hash,
       subscription_price_id,
       type,
       entry_datetime,
       from_date,
       to_date,
       price,
       cancelled,
       cancel_datetime,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_subscription_price_inserts.bk_hash,
       #s_exerp_subscription_price_inserts.subscription_price_id,
       #s_exerp_subscription_price_inserts.type,
       #s_exerp_subscription_price_inserts.entry_datetime,
       #s_exerp_subscription_price_inserts.from_date,
       #s_exerp_subscription_price_inserts.to_date,
       #s_exerp_subscription_price_inserts.price,
       #s_exerp_subscription_price_inserts.cancelled,
       #s_exerp_subscription_price_inserts.cancel_datetime,
       #s_exerp_subscription_price_inserts.ets,
       #s_exerp_subscription_price_inserts.dummy_modified_date_time,
       case when s_exerp_subscription_price.s_exerp_subscription_price_id is null then isnull(#s_exerp_subscription_price_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_subscription_price_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_subscription_price_inserts
  left join p_exerp_subscription_price
    on #s_exerp_subscription_price_inserts.bk_hash = p_exerp_subscription_price.bk_hash
   and p_exerp_subscription_price.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_subscription_price
    on p_exerp_subscription_price.bk_hash = s_exerp_subscription_price.bk_hash
   and p_exerp_subscription_price.s_exerp_subscription_price_id = s_exerp_subscription_price.s_exerp_subscription_price_id
 where s_exerp_subscription_price.s_exerp_subscription_price_id is null
    or (s_exerp_subscription_price.s_exerp_subscription_price_id is not null
        and s_exerp_subscription_price.dv_hash <> #s_exerp_subscription_price_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_subscription_price @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_subscription_price @current_dv_batch_id

end
