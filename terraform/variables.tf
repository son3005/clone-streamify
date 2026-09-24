variable "project" {
    description = "GCP Project ID"
    default     = ""
}

variable "region" {
    description = "GCP Region"
    default     = "us-central1"
    type        = string
}

variable "zone" {
    description = "GCP Zone"
    default     = "us-central1-a"
    type        = string
}

variable "storage_class" {
    description = "Storage Class for the bucket"
    default     = "STANDARD"
    type        = string
}

variable "vm_image" {
    description = "Image for you VM"
    default     = "default"
    type        = string
}


variable "network" {
    description = "Network for your instance/cluster"
    default     = "default"
    type        = string
}

variable "stg_bq_dataset" {
    description = "Storage class type for your bucket. Check official docs for more info."
    default     = "streamify_stg"
    type        = string
}

variable "prod_bq_dataset" {
    description = "Storage class type for your bucket. Check official docs for more info."
    default     = "streamify_stg"
    type        = string
}

variable "bucket" {
    description = "The Name of your bucket. This should be unique arcross GCP."
    type        = string
}