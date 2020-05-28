CREATE PROC [dbo].[proc_wrk_pega_guest_club_usage] @dv_batch_id [varchar](500) AS
begin

	set nocount on
	set xact_abort on

	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
    DECLARE @load_dv_batch_id BIGINT =  (select isnull(max(@dv_batch_id), -1) from wrk_pega_guest_club_usage)    

    if object_id('tempdb..#guest_club_usage') is not null drop table #guest_club_usage
    create table dbo.#guest_club_usage with (distribution = hash (guest_of_dim_mms_member_key),location = user_db) as
    select 
        fact_mms_guest_club_usage.guest_visit_id,
        convert(varchar(16),d_mms_guest_visit.visit_date_time,120) check_in_date_time,
        dim_club.club_id club_id,
        d_mms_guest_privilege_rule.guest_privilege_rule_id guest_privilege_rule_id,
        d_mms_guest_privilege_rule.max_number_of_guests max_number_of_guests,
        d_mms_guest_visit.member_id guest_of_member_id,
        d_mms_guest_visit.dim_mms_member_key guest_of_dim_mms_member_key,
        fact_mms_guest_club_usage.membership_id,
        d_mms_guest.guest_id
        from fact_mms_guest_club_usage
        left join dim_club dim_club
        on fact_mms_guest_club_usage.dim_club_key = dim_club.dim_club_key
        left join d_mms_guest_visit
        on fact_mms_guest_club_usage.fact_mms_guest_club_usage_key =  d_mms_guest_visit.bk_hash 
        left join d_mms_guest
        on fact_mms_guest_club_usage.dim_club_guest_key =  d_mms_guest.dim_club_guest_key 
        left join d_mms_guest_privilege_rule d_mms_guest_privilege_rule
        on fact_mms_guest_club_usage.dim_mms_membership_guest_privilege_rule_key = d_mms_guest_privilege_rule.dim_mms_membership_guest_privilege_rule_key
        where dim_mms_member_key not in ('-997','-998','-999') and fact_mms_guest_club_usage.dv_batch_id >= @load_dv_batch_id
        and fact_mms_guest_club_usage.check_in_dim_date_key >= '20190101'
		group by fact_mms_guest_club_usage.guest_visit_id,
		 d_mms_guest_visit.visit_date_time,
		 dim_club.club_id,
         d_mms_guest_privilege_rule.guest_privilege_rule_id,
         d_mms_guest_privilege_rule.max_number_of_guests,
         d_mms_guest_visit.member_id ,
		 d_mms_guest_visit.dim_mms_member_key,
		 fact_mms_guest_club_usage.membership_id,
		 d_mms_guest.guest_id
	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.wrk_pega_guest_club_usage 	WHERE dv_batch_id = @current_dv_batch_id

	INSERT INTO wrk_pega_guest_club_usage (
		guest_visit_id,
		check_in_date_time,
		club_id,
        guest_privilege_rule_id,
        max_number_of_guests,
        guest_of_member_id ,
		guest_of_dim_mms_member_key,
		membership_id,
		guest_id
		,sequence_number
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
	)
	SELECT           
		 guest_visit_id,
		 check_in_date_time,
		 club_id,
         guest_privilege_rule_id,
         max_number_of_guests,
         guest_of_member_id ,
		 guest_of_dim_mms_member_key,
		 membership_id,
		 guest_id
		,row_number() over(partition by @current_dv_batch_id order by guest_of_member_id )
		,@current_dv_batch_id
		,getdate()
		,suser_sname()
	FROM #guest_club_usage

	COMMIT TRAN
END
