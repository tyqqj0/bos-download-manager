#!/usr/bin/env python3
"""BCC Worker Provisioning Script — create and set up new BJ download workers.

Usage (run on S1):
    # Create 4 new workers starting from bj5
    python3 scripts/bcc_provision.py create --count 4 --start-index 5

    # List all instances
    python3 scripts/bcc_provision.py list

    # Set up an existing instance as a DLM worker
    python3 scripts/bcc_provision.py setup --ip 120.48.148.25 --key bj5

    # Delete an instance
    python3 scripts/bcc_provision.py delete --instance-id i-XXXXX
"""

import argparse
import json
import os
import subprocess
import sys
import time

SPEC = "bcc.e1.c8m16"
IMAGE_ID = "m-0uYcInSi"           # Ubuntu 24.04 LTS
ZONE = "cn-bj-d"
SUBNET_ID = "sbn-n29bwncn917m"
VPC_ID = "vpc-ywjgczhir4eq"
ENT_SG_ID = "esg-ejn2ukr4bxw2"
KEYPAIR_ID = "k-lBMSbBLA"         # macmini
DISK_GB = 500
DISK_TYPE = "cloud_hp1"
BANDWIDTH_MBPS = 200
TEMPORAL_SERVER = "154.85.43.52"
REPO_DIR = "/root/code/bos-download-manager"


def get_client():
    from baidubce.bce_client_configuration import BceClientConfiguration
    from baidubce.auth.bce_credentials import BceCredentials
    from baidubce.services.bcc import bcc_client

    ak = os.environ.get("BAIDU_AK") or _load_env().get("BAIDU_AK", "")
    sk = os.environ.get("BAIDU_SK") or _load_env().get("BAIDU_SK", "")
    if not ak or not sk:
        sys.exit("ERROR: BAIDU_AK / BAIDU_SK not found in environment or .env")

    config = BceClientConfiguration(
        credentials=BceCredentials(ak, sk),
        endpoint="bcc.bj.baidubce.com",
    )
    return bcc_client.BccClient(config)


def _load_env():
    env = {}
    for path in ["/root/.env", ".env"]:
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        env[k.strip()] = v.strip()
    return env


def cmd_list(args):
    client = get_client()
    resp = client.list_instances()
    instances = resp.instances if hasattr(resp, "instances") else []
    print(f"{'Name':<25} {'ID':<15} {'Status':<10} {'Public IP':<18} {'Spec':<16} {'BW'}")
    print("-" * 100)
    for inst in sorted(instances, key=lambda x: x.name or ""):
        bw = getattr(inst, "network_capacity_in_mbps", "?")
        print(f"{inst.name:<25} {inst.id:<15} {inst.status:<10} {inst.public_ip or '-':<18} {inst.spec:<16} {bw}Mbps")


def cmd_create(args):
    from baidubce.services.bcc import bcc_model

    client = get_client()
    count = args.count
    start = args.start_index

    billing = bcc_model.Billing(
        paymentTiming="Postpaid",
    )

    names = [f"bj{start + i}" for i in range(count)]
    print(f"Creating {count} instances: {', '.join(names)}")
    print(f"  Spec: {SPEC}, Disk: {DISK_GB}GB {DISK_TYPE}, BW: {BANDWIDTH_MBPS}Mbps")

    created = []
    for i, name in enumerate(names):
        print(f"\n[{i+1}/{count}] Creating {name}...")
        try:
            resp = client.create_instance_by_spec(
                spec=SPEC,
                image_id=IMAGE_ID,
                root_disk_size_in_gb=DISK_GB,
                root_disk_storage_type=DISK_TYPE,
                network_capacity_in_mbps=BANDWIDTH_MBPS,
                purchase_count=1,
                name=name,
                billing=billing,
                zone_name=ZONE,
                subnet_id=SUBNET_ID,
                enterprise_security_group_id=ENT_SG_ID,
                key_pair_id=KEYPAIR_ID,
                client_token=f"dlm-provision-{name}-{int(time.time())}",
            )
            instance_ids = resp.instance_ids if hasattr(resp, "instance_ids") else []
            if instance_ids:
                print(f"  Created: {instance_ids[0]}")
                created.append({"name": name, "id": instance_ids[0]})
            else:
                raw = resp.__dict__ if hasattr(resp, "__dict__") else str(resp)
                print(f"  Response: {raw}")
        except Exception as e:
            print(f"  ERROR: {e}")

    if not created:
        print("\nNo instances created.")
        return

    print(f"\nWaiting for instances to start...")
    for inst in created:
        _wait_running(client, inst["id"], inst["name"])

    print("\n=== Created Instances ===")
    for inst in created:
        try:
            detail = client.get_instance(inst["id"])
            ip = detail.instance.public_ip if hasattr(detail, "instance") else "?"
            inst["ip"] = ip
            print(f"  {inst['name']}: {inst['id']} -> {ip}")
        except Exception:
            print(f"  {inst['name']}: {inst['id']} -> (IP pending)")

    if not args.no_setup:
        print("\nSetting up workers...")
        for inst in created:
            ip = inst.get("ip")
            if ip and ip != "?":
                _setup_worker(ip, inst["name"])
            else:
                print(f"  {inst['name']}: skipped (no IP yet)")


def _wait_running(client, instance_id, name, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = client.get_instance(instance_id)
            inst = resp.instance if hasattr(resp, "instance") else resp
            status = inst.status if hasattr(inst, "status") else "?"
            if status == "Running":
                print(f"  {name}: Running")
                return True
            print(f"  {name}: {status}... ({int(time.time() - start)}s)")
        except Exception as e:
            print(f"  {name}: error checking status: {e}")
        time.sleep(10)
    print(f"  {name}: TIMEOUT after {timeout}s")
    return False


def _wait_ssh(ip, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = subprocess.run(
                ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
                 "-o", "BatchMode=yes", f"root@{ip}", "echo ok"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return True
        except Exception:
            pass
        time.sleep(10)
    return False


def _setup_worker(ip, server_key):
    print(f"\n--- Setting up {server_key} ({ip}) ---")

    print(f"  Waiting for SSH...")
    if not _wait_ssh(ip):
        print(f"  ERROR: SSH not available after 120s")
        return False

    env_vars = _load_env()
    env_lines = "\n".join(f"{k}={v}" for k, v in env_vars.items()
                          if k in ("BAIDU_AK", "BAIDU_SK", "BOS_ENDPOINT", "HF_TOKEN"))

    repo_parent = os.path.dirname(REPO_DIR)
    setup_script = f"""#!/bin/bash
set -euo pipefail

echo "[$(date)] Setting up DLM worker: {server_key}"

# System deps
apt-get update -qq
apt-get install -y -qq aria2 git python3-pip tmux 2>/dev/null

# Clone repo
if [ ! -d {REPO_DIR} ]; then
    mkdir -p {repo_parent}
    git clone https://github.com/tyqqj0/bos-download-manager.git {REPO_DIR}
fi

# Install Python deps
cd {REPO_DIR}
pip install -e ".[web]" 2>&1 | tail -3

# Write .env
cat > {REPO_DIR}/.env << 'ENVEOF'
{env_lines}
ENVEOF

# Create staging dir
mkdir -p /data/staging

# SSH key for tunnel to S1
ssh-keyscan -H {TEMPORAL_SERVER} >> ~/.ssh/known_hosts 2>/dev/null || true

echo "[$(date)] Setup complete for {server_key}"
"""

    try:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", f"root@{ip}", "bash -s"],
            input=setup_script, capture_output=True, text=True, timeout=300,
        )
        print(result.stdout[-500:] if result.stdout else "  (no output)")
        if result.returncode != 0:
            print(f"  STDERR: {result.stderr[-300:]}")
            return False
    except Exception as e:
        print(f"  Setup error: {e}")
        return False

    print(f"  Syncing latest code from S1...")
    try:
        subprocess.run(
            ["rsync", "-az", "--delete",
             "--exclude", ".git", "--exclude", "__pycache__",
             "--exclude", "*.pyc", "--exclude", ".env",
             f"{REPO_DIR}/", f"root@{ip}:{REPO_DIR}/"],
            check=True, timeout=120,
        )
    except Exception as e:
        print(f"  Rsync error: {e}")

    print(f"  Starting worker daemon...")
    start_cmd = f"""
export DLM_SERVER_KEY={server_key}
cd {REPO_DIR}
pkill -f "dlm.temporal" 2>/dev/null || true
sleep 2
nohup bash scripts/start-temporal-worker.sh > /var/log/dlm-worker.log 2>&1 &
sleep 5
if pgrep -f "dlm.temporal" > /dev/null; then
    echo "Worker {server_key} started OK"
else
    echo "Worker {server_key} FAILED to start"
    tail -10 /var/log/dlm-worker.log
fi
"""
    try:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", f"root@{ip}", start_cmd],
            capture_output=True, text=True, timeout=60,
        )
        print(f"  {result.stdout.strip()}")
        if result.stderr:
            print(f"  stderr: {result.stderr[-200:]}")
    except Exception as e:
        print(f"  Start error: {e}")

    return True


def cmd_setup(args):
    _setup_worker(args.ip, args.key)


def cmd_delete(args):
    client = get_client()
    instance_id = args.instance_id
    print(f"Deleting instance {instance_id}...")
    try:
        client.release_instance(instance_id)
        print(f"  Deleted.")
    except Exception as e:
        print(f"  ERROR: {e}")


def main():
    parser = argparse.ArgumentParser(description="BCC Worker Provisioning")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="List all BCC instances")

    p_create = sub.add_parser("create", help="Create new worker instances")
    p_create.add_argument("--count", type=int, default=1, help="Number of instances")
    p_create.add_argument("--start-index", type=int, required=True, help="Starting bj index (e.g. 5 for bj5)")
    p_create.add_argument("--no-setup", action="store_true", help="Skip automatic worker setup")

    p_setup = sub.add_parser("setup", help="Set up an existing instance as DLM worker")
    p_setup.add_argument("--ip", required=True, help="Public IP of the instance")
    p_setup.add_argument("--key", required=True, help="Server key (e.g. bj5)")

    p_del = sub.add_parser("delete", help="Delete a BCC instance")
    p_del.add_argument("--instance-id", required=True, help="Instance ID to delete")

    args = parser.parse_args()
    if args.command == "list":
        cmd_list(args)
    elif args.command == "create":
        cmd_create(args)
    elif args.command == "setup":
        cmd_setup(args)
    elif args.command == "delete":
        cmd_delete(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
