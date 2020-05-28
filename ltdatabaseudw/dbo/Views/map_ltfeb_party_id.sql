CREATE VIEW [dbo].[map_ltfeb_party_id] AS select d_ltfeb_party_role.pr_party_id ltfeb_party_id,
       d_ltfeb_party_relationship_role_assignment.assigned_id,--member_id
       d_ltfeb_party_relationship.effective_from_dim_date_key,
       d_ltfeb_party_relationship.effective_to_dim_date_key,
       d_ltfeb_party_relationship_role_assignment.party_relationship_role_type--'MMS Member'
from d_ltfeb_party_role
join d_ltfeb_party_relationship
  on d_ltfeb_party_role.party_role_id = d_ltfeb_party_relationship.from_party_role_id
join d_ltfeb_party_relationship_role_assignment
  on d_ltfeb_party_relationship.party_relationship_id = d_ltfeb_party_relationship_role_assignment.party_relationship_id;