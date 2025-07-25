import copy
import json
import logging
import os
from typing import Any, Dict

import yaml

from ray.autoscaler._private.loader import load_function_or_class

logger = logging.getLogger(__name__)

# For caching provider instantiations across API calls of one python session
_provider_instances = {}

# Minimal config for compatibility with legacy-style external configs.
MINIMAL_EXTERNAL_CONFIG = {
    "available_node_types": {
        "ray.head.default": {},
        "ray.worker.default": {},
    },
    "head_node_type": "ray.head.default",
    "head_node": {},
    "worker_nodes": {},
}


def _import_aws(provider_config):
    try:
        # boto3 and botocore are imported in multiple places in the codebase,
        # so we just import them here to ensure that they are installed.
        import boto3  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "The Ray AWS VM launcher requires the AWS SDK for Python (Boto3) "
            "to be installed. You can install it with `pip install boto3`."
        ) from e

    from ray.autoscaler._private.aws.node_provider import AWSNodeProvider

    return AWSNodeProvider


def _import_gcp(provider_config):
    try:
        import googleapiclient  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "The Ray GCP VM launcher requires the Google API Client to be installed. "
            "You can install it with `pip install google-api-python-client`."
        ) from e

    from ray.autoscaler._private.gcp.node_provider import GCPNodeProvider

    return GCPNodeProvider


def _import_azure(provider_config):
    from ray.autoscaler._private._azure.node_provider import AzureNodeProvider

    return AzureNodeProvider


def _import_vsphere(provider_config):
    from ray.autoscaler._private.vsphere.node_provider import VsphereWcpNodeProvider

    return VsphereWcpNodeProvider


def _import_local(provider_config):
    if "coordinator_address" in provider_config:
        from ray.autoscaler._private.local.coordinator_node_provider import (
            CoordinatorSenderNodeProvider,
        )

        return CoordinatorSenderNodeProvider
    else:
        from ray.autoscaler._private.local.node_provider import LocalNodeProvider

        return LocalNodeProvider


def _import_readonly(provider_config):
    from ray.autoscaler._private.readonly.node_provider import ReadOnlyNodeProvider

    return ReadOnlyNodeProvider


def _import_fake_multinode(provider_config):
    from ray.autoscaler._private.fake_multi_node.node_provider import (
        FakeMultiNodeProvider,
    )

    return FakeMultiNodeProvider


def _import_fake_multinode_docker(provider_config):
    from ray.autoscaler._private.fake_multi_node.node_provider import (
        FakeMultiNodeDockerProvider,
    )

    return FakeMultiNodeDockerProvider


def _import_kuberay(provider_config):
    from ray.autoscaler._private.kuberay.node_provider import KubeRayNodeProvider

    return KubeRayNodeProvider


def _import_aliyun(provider_config):
    from ray.autoscaler._private.aliyun.node_provider import AliyunNodeProvider

    return AliyunNodeProvider


def _import_spark(provider_config):
    from ray.autoscaler._private.spark.node_provider import SparkNodeProvider

    return SparkNodeProvider


def _load_fake_multinode_defaults_config():
    import ray.autoscaler._private.fake_multi_node as ray_fake_multinode

    return os.path.join(os.path.dirname(ray_fake_multinode.__file__), "example.yaml")


def _load_read_only_defaults_config():
    import ray.autoscaler._private.readonly as ray_readonly

    return os.path.join(os.path.dirname(ray_readonly.__file__), "defaults.yaml")


def _load_fake_multinode_docker_defaults_config():
    import ray.autoscaler._private.fake_multi_node as ray_fake_multinode

    return os.path.join(
        os.path.dirname(ray_fake_multinode.__file__), "example_docker.yaml"
    )


def _load_local_defaults_config():
    import ray.autoscaler.local as ray_local

    return os.path.join(os.path.dirname(ray_local.__file__), "defaults.yaml")


def _load_aws_defaults_config():
    import ray.autoscaler.aws as ray_aws

    return os.path.join(os.path.dirname(ray_aws.__file__), "defaults.yaml")


def _load_vsphere_defaults_config():
    import ray.autoscaler.vsphere as ray_vsphere

    return os.path.join(os.path.dirname(ray_vsphere.__file__), "defaults.yaml")


def _load_gcp_defaults_config():
    import ray.autoscaler.gcp as ray_gcp

    return os.path.join(os.path.dirname(ray_gcp.__file__), "defaults.yaml")


def _load_azure_defaults_config():
    import ray.autoscaler.azure as ray_azure

    return os.path.join(os.path.dirname(ray_azure.__file__), "defaults.yaml")


def _load_aliyun_defaults_config():
    import ray.autoscaler.aliyun as ray_aliyun

    return os.path.join(os.path.dirname(ray_aliyun.__file__), "defaults.yaml")


def _import_external(provider_config):
    provider_cls = load_function_or_class(path=provider_config["module"])
    return provider_cls


_NODE_PROVIDERS = {
    "local": _import_local,
    "fake_multinode": _import_fake_multinode,
    "fake_multinode_docker": _import_fake_multinode_docker,
    "readonly": _import_readonly,
    "aws": _import_aws,
    "gcp": _import_gcp,
    "vsphere": _import_vsphere,
    "azure": _import_azure,
    "kuberay": _import_kuberay,
    "aliyun": _import_aliyun,
    "external": _import_external,  # Import an external module
    "spark": _import_spark,
}

_PROVIDER_PRETTY_NAMES = {
    "readonly": "Readonly (Manual Cluster Setup)",
    "fake_multinode": "Fake Multinode",
    "fake_multinode_docker": "Fake Multinode Docker",
    "local": "Local",
    "aws": "AWS",
    "gcp": "GCP",
    "azure": "Azure",
    "kuberay": "KubeRay",
    "aliyun": "Aliyun",
    "external": "External",
    "vsphere": "vSphere",
    "spark": "Spark",
}

_DEFAULT_CONFIGS = {
    "fake_multinode": _load_fake_multinode_defaults_config,
    "fake_multinode_docker": _load_fake_multinode_docker_defaults_config,
    "local": _load_local_defaults_config,
    "aws": _load_aws_defaults_config,
    "gcp": _load_gcp_defaults_config,
    "azure": _load_azure_defaults_config,
    "aliyun": _load_aliyun_defaults_config,
    "vsphere": _load_vsphere_defaults_config,
    "readonly": _load_read_only_defaults_config,
}


def _get_node_provider_cls(provider_config: Dict[str, Any]):
    """Get the node provider class for a given provider config.

    Note that this may be used by private node providers that proxy methods to
    built-in node providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the autoscaler config.

    Returns:
        NodeProvider class
    """
    importer = _NODE_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError(
            "Unsupported node provider: {}".format(provider_config["type"])
        )
    return importer(provider_config)


def _get_node_provider(
    provider_config: Dict[str, Any], cluster_name: str, use_cache: bool = True
) -> Any:
    """Get the instantiated node provider for a given provider config.

    Note that this may be used by private node providers that proxy methods to
    built-in node providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the autoscaler config.
        cluster_name: cluster name from the autoscaler config.
        use_cache: whether or not to use a cached definition if available. If
            False, the returned object will also not be stored in the cache.

    Returns:
        NodeProvider
    """
    provider_key = (json.dumps(provider_config, sort_keys=True), cluster_name)
    if use_cache and provider_key in _provider_instances:
        return _provider_instances[provider_key]

    provider_cls = _get_node_provider_cls(provider_config)
    new_provider = provider_cls(provider_config, cluster_name)

    if use_cache:
        _provider_instances[provider_key] = new_provider

    return new_provider


def _clear_provider_cache():
    global _provider_instances
    _provider_instances = {}


def _get_default_config(provider_config):
    """Retrieve a node provider.

    This is an INTERNAL API. It is not allowed to call this from any Ray
    package outside the autoscaler.
    """
    if provider_config["type"] == "external":
        return copy.deepcopy(MINIMAL_EXTERNAL_CONFIG)
    load_config = _DEFAULT_CONFIGS.get(provider_config["type"])
    if load_config is None:
        raise NotImplementedError(
            "Unsupported node provider: {}".format(provider_config["type"])
        )
    path_to_default = load_config()
    with open(path_to_default) as f:
        defaults = yaml.safe_load(f)

    return defaults
