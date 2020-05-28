CREATE PROC [marketing].[proc_prompt_region_club] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

select  distinct dim_club.dim_club_key dim_club_key,
dim_club.club_code,
dim_club.club_code+' - '+dim_club.club_name club_code_dash_name,
r_mms_val_region.description mms_region_name,
dim_club.club_id mms_club_id  
from marketing.v_dim_club dim_club 
join r_mms_val_region on
dim_club.val_region_id = r_mms_val_region.val_region_id 
where dim_club.club_id not in (-1,99,100)
and dim_club.club_id < 900
and dim_club.club_type='club'
and dim_club.club_status IN ('Open','PreSale')
and dim_club.club_id in (select club_id from p_mms_club)
order by dim_club_key

end