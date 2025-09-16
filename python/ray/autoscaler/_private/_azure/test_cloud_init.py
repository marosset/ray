#!/usr/bin/env python3
"""
Unit tests for Azure cloud-init functionality.

Notes:
- These tests import the cloud-init generation helper directly from file to
  avoid importing the full `ray` package (which can load native extensions)
  during collection.

- To run with pytest without importing the `ray` package, use:

    pytest --import-mode=importlib python/ray/autoscaler/_private/_azure/test_cloud_init.py

  The `--import-mode=importlib` flag forces pytest to import the test module
  from the file location instead of importing it as a package submodule,
  which prevents execution of `ray/__init__.py` during collection.

- To run under Bazel use:

    bazel test //python/ray/autoscaler/_private/_azure:test_cloud_init --test_output=all

"""

import base64
import importlib.util
import json
import os
import unittest


class TestCloudInitGeneration(unittest.TestCase):
    """Unit tests for cloud-init generation logic."""

    def setUp(self):
        """Set up test by loading the cloud_init helper from file."""
        module_path = os.path.join(os.path.dirname(__file__), "cloud_init.py")
        spec = importlib.util.spec_from_file_location("azure_cloud_init", module_path)
        cloud_init = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cloud_init)
        self.generate_cloud_init_data = cloud_init.generate_cloud_init_data

    def test_cloud_init_with_disk_size(self):
        """Test that cloud-init data is generated when OS disk size is specified."""
        template_params = {"osDiskSize": 100}

        # Call the actual method
        result = self.generate_cloud_init_data(template_params)

        self.assertNotEqual(result, "")

        # Decode and verify the script content
        decoded = base64.b64decode(result).decode("utf-8")
        self.assertIn("#cloud-config", decoded)
        self.assertIn("cloud-utils-growpart", decoded)
        self.assertIn("growpart", decoded)
        self.assertIn("resize2fs", decoded)
        self.assertIn("xfs_growfs", decoded)

    def test_cloud_init_without_disk_size(self):
        """Test that no cloud-init data is generated when OS disk size is not specified."""
        template_params = {}

        result = self.generate_cloud_init_data(template_params)
        self.assertEqual(result, "")

    def test_cloud_init_with_zero_disk_size(self):
        """Test that no cloud-init data is generated when OS disk size is zero."""
        template_params = {"osDiskSize": 0}

        result = self.generate_cloud_init_data(template_params)
        self.assertEqual(result, "")

    def test_cloud_init_with_negative_disk_size(self):
        """Test that no cloud-init data is generated when OS disk size is negative."""
        template_params = {"osDiskSize": -10}

        result = self.generate_cloud_init_data(template_params)
        self.assertEqual(result, "")

    def test_cloud_init_script_structure(self):
        """Test that the generated cloud-init script has the expected structure."""
        template_params = {"osDiskSize": 200}

        result = self.generate_cloud_init_data(template_params)
        decoded = base64.b64decode(result).decode("utf-8")

        # Verify cloud-config structure
        self.assertTrue(decoded.startswith("#cloud-config"))
        self.assertIn("package_update: true", decoded)
        self.assertIn("packages:", decoded)
        self.assertIn("- cloud-utils-growpart", decoded)
        self.assertIn("runcmd:", decoded)
        self.assertIn("final_message:", decoded)


class TestArmTemplateIntegration(unittest.TestCase):
    """Unit tests for ARM template integration."""

    def test_arm_template_customdata_parameter(self):
        """Test that the ARM template properly accepts customData parameter."""
        template_path = os.path.join(
            os.path.dirname(__file__), "azure-vm-template.json"
        )

        if not os.path.exists(template_path):
            self.skipTest("ARM template file not found")

        with open(template_path, "r") as f:
            template = json.load(f)

        # Check that customData parameter exists
        self.assertIn("customData", template["parameters"])

        # Check parameter type and default
        custom_data_param = template["parameters"]["customData"]
        self.assertEqual(custom_data_param["type"], "string")
        self.assertEqual(custom_data_param["defaultValue"], "")

    def test_arm_template_vm_uses_customdata(self):
        """Test that the VM resource uses the customData parameter."""
        template_path = os.path.join(
            os.path.dirname(__file__), "azure-vm-template.json"
        )

        if not os.path.exists(template_path):
            self.skipTest("ARM template file not found")

        with open(template_path, "r") as f:
            template = json.load(f)

        # Check that customData is used in VM resource
        vm_resources = [
            r
            for r in template["resources"]
            if r["type"] == "Microsoft.Compute/virtualMachines"
        ]
        self.assertGreater(len(vm_resources), 0, "No VM resources found in template")

        vm_resource = vm_resources[0]
        self.assertIn("osProfile", vm_resource["properties"])

        # Check for customData usage in osProfile
        os_profile_str = json.dumps(vm_resource["properties"]["osProfile"])
        self.assertIn("customData", os_profile_str)


if __name__ == "__main__":
    unittest.main()
