CREATE PROC [dbo].[proc_etl_exerp_dw_constraint_test] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_dw_constraint_test

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_dw_constraint_test (
       bk_hash,
       test_number,
       table_1,
       table_2,
       foreign_key,
       primary_key,
       nullable,
       relationship,
       extra_con,
       test_query,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(test_number,'z#@$k%&P'))),2) bk_hash,
       test_number,
       table_1,
       table_2,
       foreign_key,
       primary_key,
       nullable,
       relationship,
       extra_con,
       test_query,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_dw_constraint_test.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exerp_dw_constraint_test
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_dw_constraint_test @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_dw_constraint_test (
       bk_hash,
       test_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_dw_constraint_test.bk_hash,
       stage_hash_exerp_dw_constraint_test.test_number test_number,
       isnull(cast(stage_hash_exerp_dw_constraint_test.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_dw_constraint_test
  left join h_exerp_dw_constraint_test
    on stage_hash_exerp_dw_constraint_test.bk_hash = h_exerp_dw_constraint_test.bk_hash
 where h_exerp_dw_constraint_test_id is null
   and stage_hash_exerp_dw_constraint_test.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exerp_dw_constraint_test
if object_id('tempdb..#s_exerp_dw_constraint_test_inserts') is not null drop table #s_exerp_dw_constraint_test_inserts
create table #s_exerp_dw_constraint_test_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_dw_constraint_test.bk_hash,
       stage_hash_exerp_dw_constraint_test.test_number test_number,
       stage_hash_exerp_dw_constraint_test.table_1 table_1,
       stage_hash_exerp_dw_constraint_test.table_2 table_2,
       stage_hash_exerp_dw_constraint_test.foreign_key foreign_key,
       stage_hash_exerp_dw_constraint_test.primary_key primary_key,
       stage_hash_exerp_dw_constraint_test.nullable nullable,
       stage_hash_exerp_dw_constraint_test.relationship relationship,
       stage_hash_exerp_dw_constraint_test.extra_con extra_con,
       stage_hash_exerp_dw_constraint_test.test_query test_query,
       stage_hash_exerp_dw_constraint_test.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_dw_constraint_test.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.test_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.table_1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.table_2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.foreign_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.primary_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_dw_constraint_test.nullable as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.relationship,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.extra_con,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_dw_constraint_test.test_query,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_dw_constraint_test
 where stage_hash_exerp_dw_constraint_test.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_dw_constraint_test records
set @insert_date_time = getdate()
insert into s_exerp_dw_constraint_test (
       bk_hash,
       test_number,
       table_1,
       table_2,
       foreign_key,
       primary_key,
       nullable,
       relationship,
       extra_con,
       test_query,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_dw_constraint_test_inserts.bk_hash,
       #s_exerp_dw_constraint_test_inserts.test_number,
       #s_exerp_dw_constraint_test_inserts.table_1,
       #s_exerp_dw_constraint_test_inserts.table_2,
       #s_exerp_dw_constraint_test_inserts.foreign_key,
       #s_exerp_dw_constraint_test_inserts.primary_key,
       #s_exerp_dw_constraint_test_inserts.nullable,
       #s_exerp_dw_constraint_test_inserts.relationship,
       #s_exerp_dw_constraint_test_inserts.extra_con,
       #s_exerp_dw_constraint_test_inserts.test_query,
       #s_exerp_dw_constraint_test_inserts.dummy_modified_date_time,
       case when s_exerp_dw_constraint_test.s_exerp_dw_constraint_test_id is null then isnull(#s_exerp_dw_constraint_test_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_dw_constraint_test_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_dw_constraint_test_inserts
  left join p_exerp_dw_constraint_test
    on #s_exerp_dw_constraint_test_inserts.bk_hash = p_exerp_dw_constraint_test.bk_hash
   and p_exerp_dw_constraint_test.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_dw_constraint_test
    on p_exerp_dw_constraint_test.bk_hash = s_exerp_dw_constraint_test.bk_hash
   and p_exerp_dw_constraint_test.s_exerp_dw_constraint_test_id = s_exerp_dw_constraint_test.s_exerp_dw_constraint_test_id
 where s_exerp_dw_constraint_test.s_exerp_dw_constraint_test_id is null
    or (s_exerp_dw_constraint_test.s_exerp_dw_constraint_test_id is not null
        and s_exerp_dw_constraint_test.dv_hash <> #s_exerp_dw_constraint_test_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_dw_constraint_test @current_dv_batch_id

end
