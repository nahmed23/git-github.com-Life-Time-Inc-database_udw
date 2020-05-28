CREATE PROC [dbo].[proc_util_generate_procedure_pit] @pit_table_name [varchar](500) AS
begin

set nocount on
set xact_abort on

--print '    ************************ proc_util_generate_pit_procedure start ************************'

declare @pit_proc_sql varchar(max)
declare @source_object varchar(500) = (select right(@pit_table_name,len(@pit_table_name) - 2))
declare @start int, @end int

if object_id('tempdb..#pit_columns') is not null drop table #pit_columns
create table dbo.#pit_columns with(distribution=round_robin, location=user_db) as
select source_table table_name,
       source_column column_name,
       business_key_sort_order,
       rank() over (order by sort_order) column_rank
  from dv_etl_map
 where dv_table = @pit_table_name

set @start = 1
set @end = (select max(column_rank) from #pit_columns)

declare @sat_or_link varchar(500)
declare @join_clause varchar(max) = ''
declare @column_list varchar(max) = ''
declare @qualified_column_list varchar(max) = ''
declare @dv_greatest_sql varchar(max)
declare @is int
declare @inner_sl varchar(500)
declare @process varchar(max) = 'select bk_hash'+char(13)+char(10)
                               +'  from h_'+@source_object+char(13)+char(10)
                               +' where dv_batch_id >= @process_dv_batch_id'+char(13)+char(10)

while @start <= @end
begin
    
    set @sat_or_link = (select table_name from #pit_columns where column_rank = @start and business_key_sort_order is null)
    set @column_list = @column_list +(select '        '+column_name+','+char(13)+char(10) from #pit_columns where column_rank = @start)
    set @qualified_column_list = @qualified_column_list +(select '       '+case when business_key_sort_order is not null then 'h'+'.'+column_name else 'isnull('+@sat_or_link+'.'+column_name+',''-998'')' end+','+char(13)+char(10) from #pit_columns where column_rank = @start)

    if @sat_or_link is not null
    begin
	    set @is = @start+1
		set @dv_greatest_sql = case when (select count(*) from #pit_columns where business_key_sort_order is null) = 1 then '       '+@sat_or_link+'.<column_name>'
			                        when @dv_greatest_sql is null then '       case when '
			                        when @is <= @end then @dv_greatest_sql+'            when '
			                        when @is > @end then @dv_greatest_sql+'            else isnull('+@sat_or_link+'.<column_name>,<null_case>) end'
									else null 
									end

		while @is <= @end
		begin
			set @inner_sl = (select table_name from #pit_columns where column_rank = @is)
			set @dv_greatest_sql = @dv_greatest_sql
			                       +isnull(case when @is<>@start+1 then 'and ' else '' end+@sat_or_link+'.<column_name> >= isnull('+@inner_sl+'.<column_name>,<null_case>) ','')
								   +isnull(case when @is = @end then 'then '+@sat_or_link+'.<column_name>'+char(13)+char(10) else '' end,'')
			set @is=@is+1
		end

        set @process = @process
                      +' union'+char(13)+char(10)
                      +'select bk_hash'+char(13)+char(10)
                      +'  from '+@sat_or_link+char(13)+char(10)
                      +' where dv_batch_id >= @process_dv_batch_id'+char(13)+char(10)

        set @join_clause = @join_clause + 
                           '  left join (select bk_hash, '+@sat_or_link+'_id, dv_batch_id, dv_load_date_time'+char(13)+char(10)
                          +'          from (select bk_hash, '+@sat_or_link+'_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,'+@sat_or_link+'_id desc) r from '+@sat_or_link+' where bk_hash in (select bk_hash from #process)) x'+char(13)+char(10)
                          +'         where r = 1) '+@sat_or_link+char(13)+char(10)
                          +'    on h.bk_hash = '+@sat_or_link+'.bk_hash'+char(13)+char(10)
    end

    set @start = @start + 1
end

set @pit_proc_sql = case when exists(select 1 from sys.procedures where name = 'proc_'+@pit_table_name) then 'alter' else 'create' end + ' procedure dbo.proc_'+@pit_table_name
          +' @current_dv_batch_id bigint'+char(13)+char(10)
          +'as'+char(13)+char(10)
          +'begin'+char(13)+char(10)
          +char(13)+char(10)
          +'set nocount on'+char(13)+char(10)
          +'set xact_abort on'+char(13)+char(10)
          +char(13)+char(10)
          +'declare @wf_name varchar(500) = ''wf_dv_'+@source_object+''''+char(13)+char(10)
          +'declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = ''Complete'')'+char(13)+char(10)
          +'declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end'+char(13)+char(10)
          +char(13)+char(10)
          +'if object_id(''tempdb..#process'') is not null drop table #process'+char(13)+char(10)
          +'create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as'+char(13)+char(10)
          +@process+char(13)+char(10)
          +'delete from '+@pit_table_name+' where bk_hash in (select bk_hash from #process)'+char(13)+char(10)
          +char(13)+char(10)
          +'insert into dbo.'+@pit_table_name+'('+char(13)+char(10)
          +'        bk_hash,'+char(13)+char(10)
          +@column_list
          +'        dv_inserted_date_time,'+char(13)+char(10)
          +'        dv_insert_user,'+char(13)+char(10)
          +'        dv_load_date_time,'+char(13)+char(10)
          +'        dv_load_end_date_time,'+char(13)+char(10)
          +'        dv_batch_id)'+char(13)+char(10)
          +'select h.bk_hash,'+char(13)+char(10)
          +@qualified_column_list
          +'       getdate(),'+char(13)+char(10)
          +'       suser_sname(),'+char(13)+char(10)
          --+'       case when s.dv_load_date_time >= l.dv_load_date_time then s.dv_load_date_time else l.dv_load_date_time end dv_Load_date_time,'+char(13)+char(10)
		  +replace(replace(@dv_greatest_sql,'<column_name>','dv_load_date_time'),'<null_case>','''jan 1, 1763''')+' dv_load_date_time,'+char(13)+char(10)
          +'       ''Dec 31, 9999'' dv_load_end_date_time,'+char(13)+char(10)
          --+'       case when s.dv_batch_id >= l.dv_batch_id then s.dv_batch_id else l.dv_batch_id end dv_batch_id'+char(13)+char(10)
          +replace(replace(@dv_greatest_sql,'<column_name>','dv_batch_id'),'<null_case>','-2')+' dv_batch_id'+char(13)+char(10)
		  +'  from h_'+@source_object+' h'+char(13)+char(10)
          +@join_clause
          +' where h.bk_hash in (select bk_hash from #process)'+char(13)+char(10)
          +'end'

--print @pit_proc_sql
exec(@pit_proc_sql)


--print '    ************************ proc_util_generate_pit_procedure end ************************'





end
