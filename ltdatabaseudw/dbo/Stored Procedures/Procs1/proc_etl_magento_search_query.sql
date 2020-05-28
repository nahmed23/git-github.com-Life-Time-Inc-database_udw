CREATE PROC [dbo].[proc_etl_magento_search_query] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_search_query

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_search_query (
       bk_hash,
       query_id,
       query_text,
       num_results,
       popularity,
       redirect,
       store_id,
       display_in_terms,
       is_active,
       is_processed,
       updated_at,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(query_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       query_id,
       query_text,
       num_results,
       popularity,
       redirect,
       store_id,
       display_in_terms,
       is_active,
       is_processed,
       updated_at,
       isnull(cast(stage_magento_search_query.updated_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_search_query
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_search_query @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_search_query (
       bk_hash,
       query_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_search_query.bk_hash,
       stage_hash_magento_search_query.query_id query_id,
       isnull(cast(stage_hash_magento_search_query.updated_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_search_query
  left join h_magento_search_query
    on stage_hash_magento_search_query.bk_hash = h_magento_search_query.bk_hash
 where h_magento_search_query_id is null
   and stage_hash_magento_search_query.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_search_query
if object_id('tempdb..#l_magento_search_query_inserts') is not null drop table #l_magento_search_query_inserts
create table #l_magento_search_query_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_search_query.bk_hash,
       stage_hash_magento_search_query.query_id query_id,
       stage_hash_magento_search_query.store_id store_id,
       isnull(cast(stage_hash_magento_search_query.updated_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.query_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.store_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_search_query
 where stage_hash_magento_search_query.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_search_query records
set @insert_date_time = getdate()
insert into l_magento_search_query (
       bk_hash,
       query_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_search_query_inserts.bk_hash,
       #l_magento_search_query_inserts.query_id,
       #l_magento_search_query_inserts.store_id,
       case when l_magento_search_query.l_magento_search_query_id is null then isnull(#l_magento_search_query_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_search_query_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_search_query_inserts
  left join p_magento_search_query
    on #l_magento_search_query_inserts.bk_hash = p_magento_search_query.bk_hash
   and p_magento_search_query.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_search_query
    on p_magento_search_query.bk_hash = l_magento_search_query.bk_hash
   and p_magento_search_query.l_magento_search_query_id = l_magento_search_query.l_magento_search_query_id
 where l_magento_search_query.l_magento_search_query_id is null
    or (l_magento_search_query.l_magento_search_query_id is not null
        and l_magento_search_query.dv_hash <> #l_magento_search_query_inserts.source_hash)

--calculate hash and lookup to current s_magento_search_query
if object_id('tempdb..#s_magento_search_query_inserts') is not null drop table #s_magento_search_query_inserts
create table #s_magento_search_query_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_search_query.bk_hash,
       stage_hash_magento_search_query.query_id query_id,
       stage_hash_magento_search_query.query_text query_text,
       stage_hash_magento_search_query.num_results num_results,
       stage_hash_magento_search_query.popularity popularity,
       stage_hash_magento_search_query.redirect redirect,
       stage_hash_magento_search_query.display_in_terms display_in_terms,
       stage_hash_magento_search_query.is_active is_active,
       stage_hash_magento_search_query.is_processed is_processed,
       stage_hash_magento_search_query.updated_at updated_at,
       isnull(cast(stage_hash_magento_search_query.updated_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.query_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_search_query.query_text,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.num_results as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.popularity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_search_query.redirect,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.display_in_terms as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.is_active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_search_query.is_processed as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_search_query.updated_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_search_query
 where stage_hash_magento_search_query.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_search_query records
set @insert_date_time = getdate()
insert into s_magento_search_query (
       bk_hash,
       query_id,
       query_text,
       num_results,
       popularity,
       redirect,
       display_in_terms,
       is_active,
       is_processed,
       updated_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_search_query_inserts.bk_hash,
       #s_magento_search_query_inserts.query_id,
       #s_magento_search_query_inserts.query_text,
       #s_magento_search_query_inserts.num_results,
       #s_magento_search_query_inserts.popularity,
       #s_magento_search_query_inserts.redirect,
       #s_magento_search_query_inserts.display_in_terms,
       #s_magento_search_query_inserts.is_active,
       #s_magento_search_query_inserts.is_processed,
       #s_magento_search_query_inserts.updated_at,
       case when s_magento_search_query.s_magento_search_query_id is null then isnull(#s_magento_search_query_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_search_query_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_search_query_inserts
  left join p_magento_search_query
    on #s_magento_search_query_inserts.bk_hash = p_magento_search_query.bk_hash
   and p_magento_search_query.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_search_query
    on p_magento_search_query.bk_hash = s_magento_search_query.bk_hash
   and p_magento_search_query.s_magento_search_query_id = s_magento_search_query.s_magento_search_query_id
 where s_magento_search_query.s_magento_search_query_id is null
    or (s_magento_search_query.s_magento_search_query_id is not null
        and s_magento_search_query.dv_hash <> #s_magento_search_query_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_search_query @current_dv_batch_id

end
