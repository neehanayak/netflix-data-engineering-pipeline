output "adls_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.adls.name
}

output "adf_name" {
  description = "Azure Data Factory name"
  value       = azurerm_data_factory.adf.name
}

output "adf_principal_id" {
  description = "Object ID of ADF's system-assigned managed identity"
  value       = azurerm_data_factory.adf.identity[0].principal_id
}
