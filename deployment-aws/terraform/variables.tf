variable "additional_cors_origins" {
  description = "Origins (including protocol and port) to include, in addition to the frontend_domain var."
  type        = string
  default     = ""
}

variable "admin_email_csv" {
  description = "A comma-delimited string of administrator emails"
  type        = string
}

variable "backend_domain" {
  description = "The domain where the API will be served from"
  type        = string
}

variable "batch_instance_ami" {
  description = "The AMI of the images used for ephemeral Batch instances. Used as a base image with a launch template."
  type        = string
  # Amazon ECS-optimized Amazon Linux 2 x86_64 AMI
  # Amazon Linux AMI 2.0.20240201 x86_64 ECS HVM GP2
  # Recommended to use an ECS-optimized AMI
  # https://www.nextflow.io/docs/latest/aws.html#create-your-custom-ami
  default     = "ami-02202da28664660ac"
}

variable "container_registry" {
  description = "The Docker container registry you wish to use."
  type        = string
  default     = "github"
}

variable "data_volume_snapshot_id" {
  description = "Snapshot ID for the data volume. Used to persist data between deployments."
  type        = string
  default     = null
}

variable "database_password" {
  description = "Password for mev database user"
  type        = string
  sensitive   = true
}

variable "database_snapshot" {
  description = "RDS snapshot ID"
  type        = string
  default     = null
}

variable "database_superuser_password" {
  description = "Root password for database"
  type        = string
  sensitive   = true
}

variable "django_settings_module" {
  description = "Settings module for the Django app"
  type        = string
  default     = "mev.settings_production"
}

variable "django_superuser_email" {
  description = "Email address to use as username for Django Admin"
  type        = string
}

variable "django_superuser_password" {
  description = "Django superuser password"
  type        = string
  sensitive   = true
}

variable "enable_remote_job_runners" {
  description = "Whether to use remote job runners like Cromwell"
  type        = string
  default     = "no"
}

variable "from_email" {
  description = "Used for the sender in registration emails. Format: Name <account@domain>"
  type        = string
}

variable "frontend_domain" {
  description = "The primary frontend domain this API will serve, do NOT include protocol"
  type        = string
}

variable "git_commit" {
  description = "Git repo code commit or branch name"
  type        = string
  default     = ""
}

variable "globus" {
  description = "Globus application configuration"
  default     = null
  type        = object({
    # app_client credentials are those related to OAuth2 flow for client-side application
    app_client_uuid        = string
    app_client_secret      = string
    # endpoint_client credentials are those related to setup/config of the Globus Connect Server
    endpoint_client_uuid   = string
    endpoint_client_secret = string
    # the UUID of the shared collection
    endpoint_id            = string
  })
}

variable "google_oauth2_client_id" {
  description = "The client ID for OAuth2 sign-in"
  type        = string
  default     = "sample_id"
}

variable "google_oauth2_client_secret" {
  description = "The client secret for OAuth2 sign-in"
  type        = string
  default     = "sample_secret"
}

variable "https_certificate_id" {
  description = "ID of the HTTPS certificate"
  type        = string
}

variable "public_data_bucket_name" {
  description = "Name of the bucket holding data for public datasets"
  type        = string
  default     = "webmev-public"
}

variable "route53_managed_zone" {
  description = "Name of the Route53 managed zone"
  type        = string
  default     = null
}

variable "sentry_url" {
  description = "The URL of the Sentry tracker. Include protocol, port"
  type        = string
  default     = ""
}

variable "storage_location" {
  description = "Where the data will be stored. One of remote or local"
  type        = string
  default     = "remote"
}

variable "ecr_image" {
  description = "Temporary for testing basic lambda function"
  type        = string
}