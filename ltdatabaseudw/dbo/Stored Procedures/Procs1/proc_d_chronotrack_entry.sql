CREATE PROC [dbo].[proc_d_chronotrack_entry] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_chronotrack_entry)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_chronotrack_entry_insert') is not null drop table #p_chronotrack_entry_insert
create table dbo.#p_chronotrack_entry_insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_entry.p_chronotrack_entry_id,
       p_chronotrack_entry.bk_hash
  from dbo.p_chronotrack_entry
 where p_chronotrack_entry.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_chronotrack_entry.dv_batch_id > @max_dv_batch_id
        or p_chronotrack_entry.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_entry.bk_hash,
       p_chronotrack_entry.entry_id entry_id,
       s_chronotrack_entry.allow_tracking allow_tracking,
       s_chronotrack_entry.apply_wave_rule apply_wave_rule,
       l_chronotrack_entry.athlete_id athlete_id,
       s_chronotrack_entry.auto_bracket_policy auto_bracket_policy,
       s_chronotrack_entry.bib bib,
       s_chronotrack_entry.ctime create_time,
       case when p_chronotrack_entry.bk_hash in('-997', '-998', '-999') then p_chronotrack_entry.bk_hash     
        when l_chronotrack_entry.race_id is null then '-998'  
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_entry.race_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_athlinks_api_vw_race_ltf_data_bk_hash,
       case when p_chronotrack_entry.bk_hash in('-997', '-998', '-999') then p_chronotrack_entry.bk_hash
           when l_chronotrack_entry.athlete_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_entry.athlete_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end d_chronotrack_athlete_bk_hash,
       l_chronotrack_entry.external_id external_id,
       s_chronotrack_entry.location_string location_string,
       s_chronotrack_entry.mtime modified_time,
       s_chronotrack_entry.name name,
       s_chronotrack_entry.notes notes,
       s_chronotrack_entry.override_bracket_rule override_bracket_rule,
       l_chronotrack_entry.prefered_bracket_id prefered_bracket_id,
       l_chronotrack_entry.primary_bracket_id primary_bracket_id,
       s_chronotrack_entry.race_age race_age,
       l_chronotrack_entry.race_id race_id,
       l_chronotrack_entry.reg_option_id reg_option_id,
       s_chronotrack_entry.reg_sms reg_sms,
       s_chronotrack_entry.reg_soc_msg reg_soc_msg,
       s_chronotrack_entry.remove_bracket remove_bracket,
       s_chronotrack_entry.search_result search_result,
       s_chronotrack_entry.status status,
       l_chronotrack_entry.team_id team_id,
       l_chronotrack_entry.trans_id trans_id,
       s_chronotrack_entry.type type,
       l_chronotrack_entry.wave_id wave_id,
       isnull(h_chronotrack_entry.dv_deleted,0) dv_deleted,
       p_chronotrack_entry.p_chronotrack_entry_id,
       p_chronotrack_entry.dv_batch_id,
       p_chronotrack_entry.dv_load_date_time,
       p_chronotrack_entry.dv_load_end_date_time
  from dbo.h_chronotrack_entry
  join dbo.p_chronotrack_entry
    on h_chronotrack_entry.bk_hash = p_chronotrack_entry.bk_hash
  join #p_chronotrack_entry_insert
    on p_chronotrack_entry.bk_hash = #p_chronotrack_entry_insert.bk_hash
   and p_chronotrack_entry.p_chronotrack_entry_id = #p_chronotrack_entry_insert.p_chronotrack_entry_id
  join dbo.l_chronotrack_entry
    on p_chronotrack_entry.bk_hash = l_chronotrack_entry.bk_hash
   and p_chronotrack_entry.l_chronotrack_entry_id = l_chronotrack_entry.l_chronotrack_entry_id
  join dbo.s_chronotrack_entry
    on p_chronotrack_entry.bk_hash = s_chronotrack_entry.bk_hash
   and p_chronotrack_entry.s_chronotrack_entry_id = s_chronotrack_entry.s_chronotrack_entry_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_chronotrack_entry
   where d_chronotrack_entry.bk_hash in (select bk_hash from #p_chronotrack_entry_insert)

  insert dbo.d_chronotrack_entry(
             bk_hash,
             entry_id,
             allow_tracking,
             apply_wave_rule,
             athlete_id,
             auto_bracket_policy,
             bib,
             create_time,
             d_athlinks_api_vw_race_ltf_data_bk_hash,
             d_chronotrack_athlete_bk_hash,
             external_id,
             location_string,
             modified_time,
             name,
             notes,
             override_bracket_rule,
             prefered_bracket_id,
             primary_bracket_id,
             race_age,
             race_id,
             reg_option_id,
             reg_sms,
             reg_soc_msg,
             remove_bracket,
             search_result,
             status,
             team_id,
             trans_id,
             type,
             wave_id,
             deleted_flag,
             p_chronotrack_entry_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         entry_id,
         allow_tracking,
         apply_wave_rule,
         athlete_id,
         auto_bracket_policy,
         bib,
         create_time,
         d_athlinks_api_vw_race_ltf_data_bk_hash,
         d_chronotrack_athlete_bk_hash,
         external_id,
         location_string,
         modified_time,
         name,
         notes,
         override_bracket_rule,
         prefered_bracket_id,
         primary_bracket_id,
         race_age,
         race_id,
         reg_option_id,
         reg_sms,
         reg_soc_msg,
         remove_bracket,
         search_result,
         status,
         team_id,
         trans_id,
         type,
         wave_id,
         dv_deleted,
         p_chronotrack_entry_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_chronotrack_entry)
--Done!
end
