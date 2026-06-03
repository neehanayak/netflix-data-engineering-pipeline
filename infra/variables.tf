variable "subscription_id" {
  description = "Azure subscription ID"
  sensitive   = true
}

variable "resource_group_name" {
  default = "rg-netflix-pipeline"
}

variable "location" {
  default = "northcentralus"
}

variable "adls_name" {
  default = "adlsnetflixpipeline"
}

variable "adf_name" {
  default = "adf-netflix-pipeline-nv"
}
