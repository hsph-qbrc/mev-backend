Install [AWS CLI](https://aws.amazon.com/cli/) and [Terraform](https://www.terraform.io/)

Set up an AWS profile (use `us-east-2` region):
```shell
aws configure --profile webmev
export AWS_PROFILE=webmev
```
If this is the first time deploying in your AWS account, [see the initial setup first](#init_setup).

Configure Terraform:
```shell
cd deployment-aws/terraform
terraform init
```
Create a new Terraform workspace, for example `dev`:
```shell
terraform workspace new dev
```
Note:
* workspace name will be used for:
  * naming AWS resources and Route53 records
  * Terraform state S3 object prefix
  * log bucket key prefix
* use workspace name `prod` for production deployments

Configure the site using `terraform.tfvars` file, for example:
```terraform
admin_email_csv = "admin@example.org"
backend_domain = "dev-mev-api.tm4.org"
django_superuser_email = "admin@example.org"
from_email = "WebMEV <noreply@mail.webmev.tm4.org>"
frontend_domain = "dev-mev.tm4.org"
ssh_key_pair_name = "dev-webmev"
storage_location = "local"
```

Create a CNAME record with the `tm4.org` domain registrar or DNS provider, for example:
```
dev-mev-api.tm4.org    CNAME    dev-mev-api.aws.tm4.org
```
where `dev` is your Terraform workspace name

Deploy the site:
```shell
terraform apply
```
Delete the site:
```shell
terraform destroy
```

<a name="init_setup"></a>
## Initial setup
The following steps need to be done only once to bootstrap the project in your AWS account.

Create a private S3 bucket named `webmev-terraform` to store Terraform state and secrets:
```shell
aws s3 mb s3://webmev-terraform --region us-east-2
aws s3api put-bucket-tagging --bucket webmev-terraform --tagging 'TagSet=[{Key=Project,Value=WebMEV}]'
```
(note that if you name the bucket as something else, you will need to modify the s3 backend in `main.tf`-- edit accordingly)

[Create an HTTPS certificate](https://docs.aws.amazon.com/acm/latest/userguide/gs-acm-request-public.html) for `*.tm4.org` in Certificate Manager

[Create a public Route53 hosted zone](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/CreatingHostedZone.html) `aws.tm4.org`

[Create a service-linked role for AWS Batch](https://docs.aws.amazon.com/batch/latest/userguide/using-service-linked-roles.html#create-slr)

## Connecting to the EC2 instances

For debugging or other reasons, you may need to connect to the API server or Cromwell EC2 instances. To track/audit connections to these servers and avoid managing SSH keys, we use AWS Systems Manager (SSM).

There are multiple ways to connect to the instance, but below we describe initiating a session using your local machine using the AWS cli (which we assume is installed).

To initiate a session/connection from your local machine, you need to [install the SSM plugin.](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)

Once that is installed, you can start a session using the following:

```shell
aws ssm start-session --target <INSTANCE_ID>
```
which will return a session ID in addition to connecting to your instance.

Note that issuing `exit` in your session will terminate the connection, but you can also close it using `aws ssm terminate-session --session-id <SESSION_ID>`.

Also note that by default, SSM will connect using Bourne shell (sh). [You can modify this preference usign the AWS cli or via console](https://aws.amazon.com/premiumsupport/knowledge-center/ssm-session-manager-change-shell/).

## Notes on redeployment

Note the following assumptions, based on the terraform plan:
- Terraform will *not* destroy the main storage and Cromwell buckets if they have any objects. Hence, `terraform destroy` will preserve the bucket-based files. This is the default behavior unless `force_destroy=true` in `terraform/storage.tf`.
- As part of destroy, terraform creates a snapshot of the EBS volume holding the data (typically mounted at `/data`).
- As part of destroy, terraform creates a snapshot the database.

If you wish to redeploy, we have the option of restoring state via `data_volume_snapshot_id` (for using a snapshot as our EBS "data volume") and `database_snapshot`. If those are not specified in your `tfvars`, they default to `null`, which means Terraform will create a new data volume and/or RDS instance.

## Notes on system alarms and rebooting the main server

Note that excessive RAM consumption can cause the main EC2 instance to enter an "alarm" state. In this case, the backend ceases to be responsive to requests. The EC2 console will report that a system reachability check has failed and needs to be rebooted. Select the main server instance and choose "reboot" from the "instance state" menu.

Once the machine is rebooted, connect to that machine (`aws ssm start-session --target <instance ID>`)and run `mkdir /tmp/supervisor`. Without that directory, the application server never comes online since Supervisor is expecting that directory to exist. After a bit (i.e. no action required other than creating that directory), if you run `supervisorctl status`, you should see:

```
celery_beat                      RUNNING   pid 1524, uptime 0:06:41
celery_worker                    RUNNING   pid 1523, uptime 0:06:41
gunicorn                         RUNNING   pid 1525, uptime 0:06:41
```
(note the status might be `STARTING` if you run that check too quickly. )

## Incorporating Globus

If you wish to incorporate Globus into WebMeV, see the [Globus setup instructions](./globus_setup.md)
