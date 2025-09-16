import base64
import logging

logger = logging.getLogger(__name__)


def generate_cloud_init_data(template_params: dict) -> str:
    """Generate base64-encoded cloud-init data for disk expansion if needed.

    This is a pure-Python helper with no external dependencies so it can be
    imported and unit-tested without pulling in Ray/azure SDKs or native
    extensions.
    """
    # Check if osDiskSize is specified and > 0
    os_disk_size = template_params.get("osDiskSize", 0)
    if not os_disk_size or os_disk_size <= 0:
        return ""

    logger.info(
        f"Generating cloud-init script for OS disk expansion (size: {os_disk_size}GB)"
    )

    # Inline disk expansion script
    disk_expansion_script = """#!/bin/bash
set -euo pipefail

echo "[$(date)] Starting Ray Azure OS disk expansion" | logger -t ray-disk-expansion

# Install growpart if needed
if ! command -v growpart &> /dev/null; then
    export DEBIAN_FRONTEND=noninteractive
    if command -v apt-get &> /dev/null; then
        apt-get update -qq
        apt-get install -y -qq cloud-utils-growpart
    elif command -v yum &> /dev/null; then
        yum install -y cloud-utils-growpart
    elif command -v dnf &> /dev/null; then
        dnf install -y cloud-utils-growpart
    else
        echo "ERROR: Package manager not supported" | logger -t ray-disk-expansion
        exit 1
    fi
fi

# Find root device and expand partition
root_device=$(df / | tail -1 | awk '{print $1}')
echo "Root device: $root_device" | logger -t ray-disk-expansion

if [[ $root_device =~ ^(.+[^0-9])([0-9]+)$ ]]; then
    device_name="${BASH_REMATCH[1]}"
    partition_num="${BASH_REMATCH[2]}"

    echo "Expanding partition $partition_num on $device_name" | logger -t ray-disk-expansion
    growpart "$device_name" "$partition_num" || echo "Partition already at max size" | logger -t ray-disk-expansion

    # Expand filesystem
    fs_type=$(df -T / | tail -1 | awk '{print $2}')
    case $fs_type in
        ext2|ext3|ext4)
            resize2fs "$root_device"
            ;;
        xfs)
            xfs_growfs /
            ;;
        *)
            echo "WARNING: Unsupported filesystem type $fs_type" | logger -t ray-disk-expansion
            ;;
    esac

    echo "Disk expansion completed successfully" | logger -t ray-disk-expansion
    df -h / | logger -t ray-disk-expansion
else
    echo "ERROR: Could not parse device name from $root_device" | logger -t ray-disk-expansion
    exit 1
fi"""

    # Create cloud-init configuration
    cloud_init_config = f"""#cloud-config
package_update: true
packages:
  - cloud-utils-growpart

runcmd:
  - |
{disk_expansion_script}

final_message: "Ray Azure VM with disk expansion completed successfully"
"""

    # Encode as base64 for Azure ARM template
    cloud_init_b64 = base64.b64encode(cloud_init_config.encode("utf-8")).decode("utf-8")
    logger.debug(f"Generated cloud-init data (length: {len(cloud_init_b64)} chars)")

    return cloud_init_b64
