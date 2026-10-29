"""
ECS Fargate deployment of the Taxi Time Machine into the VPC peered with the Redpanda
BYOC cluster.

  cdk deploy -c vpc_id=vpc-0123 [-c public=true] [-c secret_name=taxi-time-machine]

The Secrets Manager secret is a JSON object with the same keys as .env.example
(RP_BOOTSTRAP, RP_SR_URL, RP_USER, RP_PASS, RP_SQL_HOST, RP_SQL_AUTH, RP_SQL_USER,
RP_SQL_PASSWORD). Create it before deploying.
"""
import os

import aws_cdk as cdk
from aws_cdk import Duration, aws_ec2 as ec2, aws_ecr_assets as ecr_assets, aws_ecs as ecs
from aws_cdk import aws_ecs_patterns as patterns, aws_secretsmanager as sm
from constructs import Construct

REPO_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
SECRET_KEYS = ["RP_BOOTSTRAP", "RP_SR_URL", "RP_USER", "RP_PASS",
               "RP_SQL_HOST", "RP_SQL_AUTH", "RP_SQL_USER", "RP_SQL_PASSWORD"]


class TaxiTimeMachineStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        vpc_id = self.node.try_get_context("vpc_id")
        if not vpc_id:
            raise ValueError("pass the VPC peered with the Redpanda cluster: cdk deploy -c vpc_id=vpc-...")
        public = str(self.node.try_get_context("public")).lower() == "true"
        secret = sm.Secret.from_secret_name_v2(self, "Settings", self.node.try_get_context("secret_name"))

        vpc = ec2.Vpc.from_lookup(self, "Vpc", vpc_id=vpc_id)
        cluster = ecs.Cluster(self, "Cluster", vpc=vpc)

        service = patterns.ApplicationLoadBalancedFargateService(
            self, "App",
            cluster=cluster,
            cpu=512,
            memory_limit_mib=1024,
            desired_count=1,
            public_load_balancer=public,
            task_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            runtime_platform=ecs.RuntimePlatform(cpu_architecture=ecs.CpuArchitecture.X86_64,
                                                 operating_system_family=ecs.OperatingSystemFamily.LINUX),
            task_image_options=patterns.ApplicationLoadBalancedTaskImageOptions(
                image=ecs.ContainerImage.from_asset(REPO_ROOT, platform=ecr_assets.Platform.LINUX_AMD64),
                container_port=8080,
                environment={"RP_SQL_PORT": "5432", "RP_SQL_DB": "oxla"},
                secrets={k: ecs.Secret.from_secrets_manager(secret, k) for k in SECRET_KEYS},
            ),
            idle_timeout=Duration.minutes(5),  # keep the live SSE stream open
        )
        service.target_group.configure_health_check(path="/api/health")
        service.target_group.set_attribute("deregistration_delay.timeout_seconds", "10")
        cdk.CfnOutput(self, "Url", value=f"http://{service.load_balancer.load_balancer_dns_name}")


app = cdk.App()
TaxiTimeMachineStack(app, "TaxiTimeMachine", env=cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"), region=os.environ.get("CDK_DEFAULT_REGION")))
app.synth()
