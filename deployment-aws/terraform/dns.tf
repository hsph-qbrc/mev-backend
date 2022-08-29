data "aws_route53_zone" "main" {
  name = "aws.tm4.org"
}

locals {
  backend_cname  = "${local.stack}-mev-api.${data.aws_route53_zone.main.name}"
  cromwell_cname = "${local.stack}-cromwell.${data.aws_route53_zone.main.name}"
  frontend_cname = "${local.stack}-mev.${data.aws_route53_zone.main.name}"
}

resource "aws_route53_record" "web" {
  name    = local.backend_cname
  type    = "A"
  zone_id = data.aws_route53_zone.main.zone_id
  alias {
    evaluate_target_health = true
    name                   = aws_lb.api.dns_name
    zone_id                = aws_lb.api.zone_id
  }
}

resource "aws_route53_record" "web6" {
  name    = local.backend_cname
  type    = "AAAA"
  zone_id = data.aws_route53_zone.main.zone_id
  alias {
    evaluate_target_health = true
    name                   = aws_lb.api.dns_name
    zone_id                = aws_lb.api.zone_id
  }
}
