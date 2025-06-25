#!/bin/bash
# ena-install.sh
# Universal ENA Driver Installation and Bootloader Configuration Script
# This script installs/updates the custom ENA driver from source (using the amzn-drivers repository),
# backs up and replaces the vendor-supplied module, updates the initramfs,
# and configures bootloader and modprobe settings so that the new driver is used at boot.
#
# Note: This version does not force the "force_large_llq_header=0" parameter, which is typically used
# for ENA Express. For regular ENA, this is not required.
#
# Usage:
#   sudo ./ena-install.sh --apply    # Install/update custom ENA driver and apply configurations
#   sudo ./ena-install.sh --revert     # Rollback changes and restore vendor-supplied module
#
# References:
#   https://github.com/amzn/amzn-drivers/tree/master/kernel/linux/ena
#   https://github.com/amzn/amzn-drivers/blob/master/kernel/linux/ena/ENA_Linux_Best_Practices.rst
#   https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/enhanced-networking-ena.html
#   https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-express.html
#   https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-improve-network-latency-linux.html
#   https://github.com/amzn/amzn-drivers/blob/master/kernel/linux/ena/RELEASENOTES.md
#
# Note: Ensure you have a backup or AMI snapshot before applying these changes.
set -e

usage() {
    echo "Usage: $0 --apply | --revert"
    exit 1
}

if [ "$#" -ne 1 ]; then
    usage
fi

MODE="$1"

# Ensure the script is run as root.
if [[ $(id -u) -ne 0 ]]; then
    echo "Error: Please run this script as root." >&2
    exit 1
fi

#############################################
# Global Variables
#############################################
MODULE_DIR="/lib/modules/$(uname -r)/kernel/drivers/net/ethernet/amazon/ena"
CUSTOM_MODULE="$MODULE_DIR/ena.ko"
BACKUP_MODULE="$MODULE_DIR/ena.ko.vendor_backup"
TMPDIR_BUILD=$(mktemp -d)
DESIRED_TAG="ena_linux_2.13.2"
MODPROBE_CONFIG="/etc/modprobe.d/10-unsupported-modules.conf"
MODULES_LOAD="/etc/modules-load.d/ena.conf"
GRUB_CONFIG="/etc/default/grub"

#############################################
# Function: check_kernel_headers
# Checks if the kernel build directory exists; if not, automatically attempts to install the matching kernel headers.
#############################################
check_kernel_headers() {
    if [ ! -d "/lib/modules/$(uname -r)/build" ]; then
        echo "Kernel build directory /lib/modules/$(uname -r)/build not found."
        echo "Attempting to install the matching kernel development package..."
        if command -v zypper >/dev/null 2>&1; then
            # Try installing the versioned package first, then fallback.
            sudo zypper --non-interactive install kernel-default-devel-$(uname -r) || sudo zypper --non-interactive install kernel-default-devel
        elif command -v apt-get >/dev/null 2>&1; then
            sudo apt-get update && sudo apt-get install -y linux-headers-$(uname -r)
        elif command -v yum >/dev/null 2>&1; then
            sudo yum install -y kernel-devel-$(uname -r)
        else
            echo "No supported package manager found. Aborting."
            exit 1
        fi
        # Re-check after installation attempt.
        if [ ! -d "/lib/modules/$(uname -r)/build" ]; then
            echo "Kernel build directory still not found after installation attempt."
            exit 1
        else
            echo "Kernel build directory now exists: /lib/modules/$(uname -r)/build"
        fi
    else
        echo "Kernel build directory exists: /lib/modules/$(uname -r)/build"
    fi
}

#############################################
# Function: update_initramfs
#############################################
update_initramfs() {
    if command -v dracut >/dev/null 2>&1; then
        echo "Updating initramfs using dracut..."
        dracut -f -v
    elif command -v update-initramfs >/dev/null 2>&1; then
        echo "Updating initramfs using update-initramfs..."
        update-initramfs -u
    else
        echo "Warning: No initramfs update tool found. Please update initramfs manually."
    fi
}

#############################################
# Function: configure_bootloader
# Disables predictable network interface names.
#############################################
configure_bootloader() {
    if [ -f "$GRUB_CONFIG" ]; then
        echo "Configuring bootloader to disable predictable network interface names..."
        if grep -q "net.ifnames=0" "$GRUB_CONFIG"; then
            echo "net.ifnames=0 already set in $GRUB_CONFIG"
        else
            sed -i '/^GRUB_CMDLINE_LINUX/s/"$/ net.ifnames=0"/' "$GRUB_CONFIG"
            echo "Updated $GRUB_CONFIG with net.ifnames=0"
        fi
        if command -v grub2-mkconfig >/dev/null 2>&1; then
            grub2-mkconfig -o /boot/grub2/grub.cfg
        elif command -v update-grub >/dev/null 2>&1; then
            update-grub
        else
            echo "Warning: Could not update grub configuration. Please update manually."
        fi
    else
        echo "$GRUB_CONFIG not found. Skipping bootloader configuration."
    fi
}

#############################################
# Function: configure_modprobe
# Configures modprobe to allow unsupported modules only on SLES.
#############################################
configure_modprobe() {
    echo "Configuring modprobe settings..."
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        if [ "$ID" = "sles" ]; then
            echo "Detected SLES. Writing allow_unsupported_modules setting."
            echo "allow_unsupported_modules 1" >"$MODPROBE_CONFIG"
        else
            echo "Non-SLES distribution detected; skipping allow_unsupported_modules setting."
            rm -f "$MODPROBE_CONFIG" 2>/dev/null || true
        fi
    else
        echo "OS release information not found; skipping modprobe configuration."
    fi
}

#############################################
# Function: install_ena_driver
# Clones, builds, and installs the custom ENA driver.
# Also backs up and replaces the vendor-supplied module.
#############################################
install_ena_driver() {
    echo "Installing/updating the custom ENA driver..."

    # Ensure kernel headers are present.
    check_kernel_headers

    # Install build dependencies.
    if command -v zypper >/dev/null 2>&1; then
        echo "Installing dependencies with zypper..."
        if zypper search -s dkms | grep -q "^dkms"; then
            zypper --non-interactive install -y make gcc kernel-default-devel git dkms
        else
            zypper --non-interactive install -y make gcc kernel-default-devel git
        fi
    elif command -v apt-get >/dev/null 2>&1; then
        echo "Installing dependencies with apt-get..."
        apt-get update
        apt-get install -y make gcc linux-headers-$(uname -r) git dkms
    elif command -v yum >/dev/null 2>&1; then
        echo "Installing dependencies with yum..."
        yum install -y make gcc kernel-devel-$(uname -r) git dkms
    else
        echo "No supported package manager found. Aborting."
        exit 1
    fi

    # Backup existing vendor-supplied module if present.
    if [ -f "$CUSTOM_MODULE" ]; then
        echo "Backing up vendor-supplied ENA module to $BACKUP_MODULE"
        cp "$CUSTOM_MODULE" "$BACKUP_MODULE"
    fi

    # Clone the repository and check out the desired tag.
    echo "Cloning amzn-drivers repository..."
    pushd "$TMPDIR_BUILD"
    git clone https://github.com/amzn/amzn-drivers.git
    cd amzn-drivers
    git fetch --tags
    if git rev-parse "$DESIRED_TAG" >/dev/null 2>&1; then
        git checkout tags/"$DESIRED_TAG" -b custom_ena
        echo "Checked out ENA driver version $DESIRED_TAG"
    else
        echo "Desired tag $DESIRED_TAG not found. Aborting."
        exit 1
    fi
    cd kernel/linux/ena
    echo "Cleaning previous build artifacts..."
    make clean || echo "No previous build artifacts."
    echo "Building the ENA driver module..."
    make
    popd

    # Ensure module directory exists.
    if [ ! -d "$MODULE_DIR" ]; then
        echo "Creating module directory $MODULE_DIR..."
        mkdir -p "$MODULE_DIR"
    fi

    echo "Installing the custom ENA driver module..."
    cp "$TMPDIR_BUILD/amzn-drivers/kernel/linux/ena/ena.ko" "$MODULE_DIR/"
    depmod -a

    # Update initramfs so that the new module is used on boot.
    update_initramfs

    # Configure modprobe to allow unsupported modules (conditionally for SLES).
    configure_modprobe

    # Ensure the module loads at boot.
    echo "ena" >"$MODULES_LOAD"

    # Unload any currently loaded ena module and load the new one.
    if lsmod | grep -q "^ena"; then
        echo "Unloading current ENA driver..."
        modprobe -r ena
    fi
    echo "Loading new ENA driver..."
    modprobe ena

    echo "Custom ENA driver installed. Version details:"
    modinfo ena | grep version
    echo "Version from sysfs: $(cat /sys/module/ena/version 2>/dev/null || echo 'Not loaded')"

    # Cleanup build directory.
    echo "Cleaning up build directory..."
    rm -rf "$TMPDIR_BUILD"
}

#############################################
# Function: rollback_ena_driver
# Reverts to the previously backed-up vendor-supplied ENA driver.
#############################################
rollback_ena_driver() {
    echo "Rolling back custom ENA driver installation..."
    if [ -f "$BACKUP_MODULE" ]; then
        echo "Removing custom ENA module..."
        rm -f "$CUSTOM_MODULE"
        echo "Restoring vendor-supplied ENA module from backup..."
        cp "$BACKUP_MODULE" "$CUSTOM_MODULE"
        depmod -a
        update_initramfs
        echo "Removing custom modprobe config and modules-load file..."
        rm -f "$MODULES_LOAD" "$MODPROBE_CONFIG"
        if lsmod | grep -q "^ena"; then
            modprobe -r ena
        fi
        modprobe ena
        echo "Rollback complete. Loaded ENA driver version:"
        modinfo ena | grep version
        echo "Version from sysfs: $(cat /sys/module/ena/version 2>/dev/null || echo 'Not loaded')"
    else
        echo "No backup module found. Cannot rollback."
        exit 1
    fi
}

#############################################
# Main Logic
#############################################
case "$MODE" in
--apply)
    install_ena_driver
    configure_bootloader
    echo "Installation and configuration complete. Please reboot your instance to verify persistent changes."
    ;;
--revert)
    rollback_ena_driver
    echo "Rollback complete. Please reboot if necessary."
    ;;
*)
    usage
    ;;
esac

exit 0
