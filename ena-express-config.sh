#!/bin/bash
# This script applies persistent network performance optimizations for ENA Express.
# It sets the MTU to 8900 by modifying the ifcfg file, configures tcp_limit_output_bytes via sysctl,
# disables BQL on TX queues (via a systemd service), sets the RX queue size to 8192 via ethtool (via a systemd service),
# and disables Large LLQ by reloading the ENA driver with force_large_llq_header=0 (only for drivers later than 2.1.0K).
#
# It checks current settings and only applies changes if needed.
# A rollback option is provided to restore previous configurations.
#
# Additionally, this script installs a persistent systemd service so that these optimizations
# are reapplied at every boot.
#
# Usage:
#   sudo ./ena-express-config.sh --apply    # Apply and persist network optimizations at boot
#   sudo ./ena-express-config.sh --revert     # Rollback network optimizations and remove persistent service
#
# Note: Ensure you have a backup of your network configuration before applying changes.
set -euo pipefail

usage() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Usage: $0 --apply | --revert" >&2
    exit 1
}

if [ "$#" -ne 1 ]; then
    usage
fi

MODE="$1"

#############################################
# Global Variables
#############################################
IFCFG="/etc/sysconfig/network/ifcfg-eth0"
IFCFG_BACKUP="/etc/sysconfig/network/ifcfg-eth0.bak"
SYSCTL_FILE="/etc/sysctl.d/99-ena-network-opt.conf"

BQL_SCRIPT="/usr/local/sbin/ena-network-opt-bql.sh"
BQL_SERVICE="/etc/systemd/system/ena-network-opt-bql.service"

ETHTOOL_SCRIPT="/usr/local/sbin/ena-network-opt-ethtool.sh"
ETHTOOL_SERVICE="/etc/systemd/system/ena-network-opt-ethtool.service"

LLQ_SCRIPT="/usr/local/sbin/ena-disable-llq.sh"
LLQ_SERVICE="/etc/systemd/system/ena-disable-llq.service"

PERSISTENT_SERVICE="/etc/systemd/system/ena-network-opt-fix.service"

#############################################
# Function: install_persistent_service
#############################################
install_persistent_service() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Installing persistent systemd service to reapply network optimizations at boot..."
    cat <<'EOF' >"$PERSISTENT_SERVICE"
[Unit]
Description=Reapply ENA Network Optimizations at Boot
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/ena-express-config.sh --apply
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable ena-network-opt-fix.service
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Persistent service ena-network-opt-fix.service installed and enabled."
}

#############################################
# Function: remove_persistent_service
#############################################
remove_persistent_service() {
    if systemctl is-enabled ena-network-opt-fix.service &>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Disabling persistent service ena-network-opt-fix.service..."
        systemctl disable ena-network-opt-fix.service
        systemctl stop ena-network-opt-fix.service
    fi
    if [ -f "$PERSISTENT_SERVICE" ]; then
        rm -f "$PERSISTENT_SERVICE"
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Persistent service file removed."
    fi
    systemctl daemon-reload
}

#############################################
# Function: apply_network_optimizations
#############################################
apply_network_optimizations() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Applying ENA Express network optimizations..."

    # 1. Set MTU to 8900 if not already set.
    CURRENT_MTU=$(ip link show eth0 | awk '/mtu/ {for(i=1;i<=NF;i++){if($i=="mtu"){print $(i+1); exit}}}')
    if [ "$CURRENT_MTU" -eq 8900 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - MTU is already 8900; skipping MTU configuration."
    else
        if [ ! -f "$IFCFG_BACKUP" ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Backing up network configuration from $IFCFG to $IFCFG_BACKUP"
            cp "$IFCFG" "$IFCFG_BACKUP" || echo "$(date '+%Y-%m-%d %H:%M:%S') - Warning: Could not backup $IFCFG"
        fi
        sed -i '/^MTU=/d' "$IFCFG"
        echo 'MTU="8900"' >>"$IFCFG"
        ip link set eth0 mtu 8900 || echo "$(date '+%Y-%m-%d %H:%M:%S') - Warning: Failed to set MTU on eth0"
        echo "$(date '+%Y-%m-%d %H:%M:%S') - MTU set to 8900 on eth0."
    fi

    # 2. Set TCP output buffer limit via sysctl.
    CURRENT_TCP=$(sysctl -n net.ipv4.tcp_limit_output_bytes 2>/dev/null || echo "unset")
    if [ "$CURRENT_TCP" == "1048576" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - TCP output buffer limit is already 1048576; skipping sysctl configuration."
    else
        echo "net.ipv4.tcp_limit_output_bytes = 1048576" >"$SYSCTL_FILE"
        sysctl -p "$SYSCTL_FILE"
        echo "$(date '+%Y-%m-%d %H:%M:%S') - TCP output buffer limit set to 1048576."
    fi

    # 3. Disable BQL on TX queues.
    CURRENT_BQL=$(cat /sys/class/net/eth0/queues/tx-0/byte_queue_limits/limit_min 2>/dev/null || echo "unknown")
    if [ "$CURRENT_BQL" == "max" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - BQL on TX queues already disabled; skipping."
    else
        cat <<'EOF' >"$BQL_SCRIPT"
#!/bin/bash
for txq in /sys/class/net/eth0/queues/tx-*; do
    echo max > "${txq}/byte_queue_limits/limit_min"
done
EOF
        chmod +x "$BQL_SCRIPT"
        cat <<'EOF' >"$BQL_SERVICE"
[Unit]
Description=Disable BQL for eth0 for ENA Express optimization
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStartPre=/bin/sleep 10
ExecStart=/usr/local/sbin/ena-network-opt-bql.sh

[Install]
WantedBy=multi-user.target
EOF
        systemctl daemon-reload
        systemctl enable ena-network-opt-bql.service
        systemctl start ena-network-opt-bql.service
        echo "$(date '+%Y-%m-%d %H:%M:%S') - BQL disabled on TX queues."
    fi

    # 4. Set RX queue size to 8192 via ethtool.
    CURRENT_RX=$(ethtool -g eth0 2>/dev/null | awk '/Current hardware settings:/{getline; print $2}')
    if [ "$CURRENT_RX" == "8192" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - RX queue size is already 8192; skipping ethtool configuration."
    else
        cat <<'EOF' >"$ETHTOOL_SCRIPT"
#!/bin/bash
echo "Setting RX ring size to 8192..."
/usr/sbin/ethtool -G eth0 rx 8192 && echo "RX ring size set to 8192." || echo "Warning: Failed to set RX ring size."
EOF
        chmod +x "$ETHTOOL_SCRIPT"
        cat <<'EOF' >"$ETHTOOL_SERVICE"
[Unit]
Description=Set ethtool RX queue size for eth0 for ENA Express optimization
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStartPre=/bin/sleep 60
ExecStart=/usr/local/sbin/ena-network-opt-ethtool.sh
SuccessExitStatus=80

[Install]
WantedBy=multi-user.target
EOF
        systemctl daemon-reload
        systemctl enable ena-network-opt-ethtool.service
        systemctl start ena-network-opt-ethtool.service
        echo "$(date '+%Y-%m-%d %H:%M:%S') - RX queue size set to 8192 via ena-network-opt-ethtool.service."
    fi

    # 5. Disable Large LLQ for drivers newer than 2.1.0K.
    DRIVER_VERSION=$(modinfo ena 2>/dev/null | awk '/^version:/ {print $2}')
    if [ "$DRIVER_VERSION" = "2.1.0K" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Detected ENA driver version $DRIVER_VERSION. force_large_llq_header is not applicable; skipping LLQ configuration."
    else
        if [ -f /sys/module/ena/parameters/force_large_llq_header ]; then
            CURRENT_LLQ=$(cat /sys/module/ena/parameters/force_large_llq_header)
        else
            CURRENT_LLQ="unknown"
        fi
        if [ "$CURRENT_LLQ" == "0" ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Large LLQ is already disabled; skipping driver reload."
        else
            cat <<'EOF' >"$LLQ_SCRIPT"
#!/bin/bash
echo "Disabling Large LLQ..."
if lsmod | grep -q "^ena"; then
    modprobe -r ena
fi
modprobe ena force_large_llq_header=0 && echo "Large LLQ disabled." || echo "Warning: Failed to disable Large LLQ."
EOF
            chmod +x "$LLQ_SCRIPT"
            cat <<'EOF' >"$LLQ_SERVICE"
[Unit]
Description=Disable Large LLQ for ena at boot
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/ena-disable-llq.sh

[Install]
WantedBy=multi-user.target
EOF
            systemctl daemon-reload
            systemctl enable ena-disable-llq.service
            /usr/local/sbin/ena-disable-llq.sh
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Large LLQ has been disabled via ena-disable-llq.service."
        fi
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') - All ENA Express network optimizations have been applied."

    # Install persistent service to reapply these settings on every boot.
    install_persistent_service
}

#############################################
# Function: rollback_network_optimizations
#############################################
rollback_network_optimizations() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Rolling back ENA Express network optimizations..."

    if [ -f "$IFCFG_BACKUP" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Restoring $IFCFG from backup..."
        cp "$IFCFG_BACKUP" "$IFCFG"
    else
        sed -i '/^MTU=/d' "$IFCFG"
        echo "$(date '+%Y-%m-%d %H:%M:%S') - MTU setting removed from $IFCFG."
    fi

    if [ -f "$SYSCTL_FILE" ]; then
        rm -f "$SYSCTL_FILE"
        sysctl --system
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Removed sysctl tuning file $SYSCTL_FILE."
    fi

    if systemctl is-enabled ena-network-opt-bql.service &>/dev/null; then
        systemctl disable ena-network-opt-bql.service
        systemctl stop ena-network-opt-bql.service
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Disabled ena-network-opt-bql.service."
    fi
    [ -f "$BQL_SERVICE" ] && rm -f "$BQL_SERVICE"
    [ -f "$BQL_SCRIPT" ] && rm -f "$BQL_SCRIPT"

    if systemctl is-enabled ena-network-opt-ethtool.service &>/dev/null; then
        systemctl disable ena-network-opt-ethtool.service
        systemctl stop ena-network-opt-ethtool.service
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Disabled ena-network-opt-ethtool.service."
    fi
    [ -f "$ETHTOOL_SERVICE" ] && rm -f "$ETHTOOL_SERVICE"
    [ -f "$ETHTOOL_SCRIPT" ] && rm -f "$ETHTOOL_SCRIPT"

    if systemctl is-enabled ena-disable-llq.service &>/dev/null; then
        systemctl disable ena-disable-llq.service
        systemctl stop ena-disable-llq.service
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Disabled ena-disable-llq.service."
    fi
    [ -f "$LLQ_SERVICE" ] && rm -f "$LLQ_SERVICE"
    [ -f "$LLQ_SCRIPT" ] && rm -f "$LLQ_SCRIPT"

    systemctl daemon-reload

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Restoring default ENA driver settings by reloading driver without force_large_llq_header..."
    if lsmod | grep -q "^ena"; then
        modprobe -r ena
    fi
    modprobe ena
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Default ENA driver settings restored."

    # Remove persistent service if it exists.
    remove_persistent_service

    echo "$(date '+%Y-%m-%d %H:%M:%S') - ENA Express network optimizations have been reverted."
}

#############################################
# Function: install_persistent_service
#############################################
install_persistent_service() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Installing persistent service to reapply optimizations at boot..."
    cat <<'EOF' >"$PERSISTENT_SERVICE"
[Unit]
Description=Reapply ENA Network Optimizations at Boot
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/ena-express-config.sh --apply
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable ena-network-opt-fix.service
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Persistent service ena-network-opt-fix.service installed and enabled."
}

#############################################
# Function: remove_persistent_service
#############################################
remove_persistent_service() {
    if systemctl is-enabled ena-network-opt-fix.service &>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Disabling persistent service ena-network-opt-fix.service..."
        systemctl disable ena-network-opt-fix.service
        systemctl stop ena-network-opt-fix.service
    fi
    if [ -f "$PERSISTENT_SERVICE" ]; then
        rm -f "$PERSISTENT_SERVICE"
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Persistent service file removed."
    fi
    systemctl daemon-reload
}

#############################################
# Main Logic
#############################################
case "$MODE" in
--apply)
    apply_network_optimizations
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Network optimization applied. A reboot may be required for all changes to persist."
    ;;
--revert)
    rollback_network_optimizations
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Network optimization rollback complete. Please reboot if necessary."
    ;;
*)
    usage
    ;;
esac

exit 0
