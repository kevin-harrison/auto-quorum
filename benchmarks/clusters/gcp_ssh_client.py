import subprocess
import time

from .gcp_cluster import GcpCluster


class GcpClusterSSHClient:
    """
    The GcpClusterSSHClient class provides a high-level interface for managing SSH-based processes
    on a Google Cloud Platform (GCP) cluster. It encapsulates the functionality to start, stop,
    restart, and monitor SSH processes across multiple cluster instances.
    """

    _processes: dict[str, tuple[subprocess.Popen, str, str]]
    _gcp_cluster: GcpCluster

    def __init__(self, gcp_cluster: GcpCluster):
        self._processes = {}
        self._gcp_cluster = gcp_cluster

    def start_process(self, process_id: str, instance_name: str, ssh_command: str):
        running_process = self._processes.get(process_id)
        if running_process is not None:
            running_process[0].terminate()
        process = self._gcp_cluster.ssh_command(instance_name, ssh_command)
        self._processes[process_id] = (process, instance_name, ssh_command)

    def start_processes(self, processes: list[tuple[str, str, str]]):
        for id, instance_name, command in processes:
            self.start_process(id, instance_name, command)

    def restart_process(self, process_id: str):
        _, instance, config = self._get_process(process_id)
        self.start_process(process_id, instance, config)

    def restart_processes(self, process_ids: list[str]):
        for id in process_ids:
            self.restart_process(id)

    def stop_process(self, process_id):
        if (process := self._processes.get(process_id)) is not None:
            process[0].terminate()
            self._processes.pop(process_id)

    def await_processes(self, process_ids: list[str], timeout: int = 600):
        print(f"Awaiting processes: {process_ids}")
        for id in process_ids:
            process, _, _ = self._get_process(id)
            process.wait(timeout=timeout)
        for id in process_ids:
            self._processes.pop(id)

    def await_processes_concurrent(
        self, process_ids: list[str], timeout: int | None = None
    ) -> bool:
        """
        Concurrently waits for processes to finish, retrying if SSH connection fails. Aborts processes if timeout (in seconds)
        is reached.

        This method retries all SSH connections up to 3 times if an instance SSH connection fails. This is necessary
        because there is an undefined delay between when an instance is created and when it becomes SSH-able. If any instance fails
        to connect, all the connections are retried.
        """
        print(f"Awaiting cluster...")
        retries = 0
        ticks = 0
        all_processes = list(self._processes.keys())
        while True:
            a_process_failed = False
            a_process_is_running = False
            for id in process_ids:
                process, _, _ = self._get_process(id)
                return_code = process.poll()
                if return_code is None:
                    a_process_is_running = True
                elif return_code == 255:
                    a_process_failed = True

            if a_process_failed:
                if retries < 3:
                    for process_id in all_processes:
                        self._processes[process_id][0].terminate()
                    time.sleep(2)
                    print(f"RETRYING CLIENT AND SERVER SSH CONNECTIONS...")
                    time.sleep(8)
                    retries += 1
                    self.restart_processes(all_processes)
                else:
                    print("Failed SSH 3 times, aborting cluster await.")
                    return False
            elif a_process_is_running:
                time.sleep(1)
                ticks += 1
                if timeout is not None and ticks > timeout:
                    print("Timeout reached, aborting cluster await.")
                    return False
            else:
                print("Cluster finished successfully.")
                return True

    def clear(self):
        self._processes.clear()

    def clear_process(self, process_id: str):
        _ = self._processes.pop(process_id)

    def _get_process(self, process_id: str) -> tuple[subprocess.Popen, str, str]:
        process = self._processes.get(process_id)
        if process is None:
            raise ValueError(f"Process {process_id} doesn't exist")
        return process
