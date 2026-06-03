# ── Resource Group ────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# ── ADLS Gen2 Storage Account ─────────────────────────────────────────────────

resource "azurerm_storage_account" "adls" {
  name                     = var.adls_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # enables hierarchical namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
}

# ── Medallion Containers ──────────────────────────────────────────────────────

resource "azurerm_storage_container" "medallion" {
  for_each = toset(["landing", "bronze", "silver", "gold"])

  name                  = each.key
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# ── Azure Data Factory ────────────────────────────────────────────────────────

resource "azurerm_data_factory" "adf" {
  name                = var.adf_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }
}

# ── RBAC: ADF → Storage Blob Data Contributor on ADLS ────────────────────────

resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}
