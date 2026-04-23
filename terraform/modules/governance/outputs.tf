# =============================================================================
# Module: Governance — Outputs
# =============================================================================

output "tag_names" {
  description = "Map of governance tag names"
  value = {
    pii              = snowflake_tag.pii.name
    sensitivity      = snowflake_tag.sensitivity_level.name
    domain           = snowflake_tag.data_domain.name
    cost_center      = snowflake_tag.cost_center.name
    retention        = snowflake_tag.retention_class.name
  }
}

output "masking_policy_names" {
  description = "Map of masking policy fully qualified names"
  value = {
    email   = snowflake_masking_policy.mask_email.name
    phone   = snowflake_masking_policy.mask_phone.name
    name    = snowflake_masking_policy.mask_name.name
    ssn     = snowflake_masking_policy.mask_ssn.name
    dob     = snowflake_masking_policy.mask_dob.name
    address = snowflake_masking_policy.mask_address.name
  }
}

output "row_access_policy_names" {
  description = "Map of row access policy names"
  value = {
    country     = snowflake_row_access_policy.country_filter.name
    sensitivity = snowflake_row_access_policy.sensitivity_filter.name
  }
}
