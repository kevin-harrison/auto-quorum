from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

INSTANCE_STARTUP_SCRIPT = BASE_DIR / "instance_startup_script.sh"
RUN_CLIENT_SCRIPT = BASE_DIR / "run_client.sh"
RUN_ETCD_SERVER_SCRIPT = BASE_DIR / "run_etcd_server.sh"
RUN_SERVER_SCRIPT = BASE_DIR / "run_server.sh"
