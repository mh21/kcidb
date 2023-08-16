"""main.py tests"""

import os
import subprocess
import unittest
from importlib import import_module
from urllib.parse import quote
import time
import yaml
import requests
import kcidb


@unittest.skipIf(os.environ.get("KCIDB_DEPLOYMENT"), "local-only")
def test_google_credentials_are_not_specified():
    """Check Google Application credentials are not specified"""
    assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is None, \
        "Local tests must run without " \
        "GOOGLE_APPLICATION_CREDENTIALS " \
        "environment variable"


def test_import():
    """Check main.py can be loaded"""
    # Load deployment environment variables
    file_dir = os.path.dirname(os.path.abspath(__file__))
    cloud_path = os.path.join(file_dir, "cloud")
    env = yaml.safe_load(
        subprocess.check_output([
            cloud_path,
            "env", "kernelci-production", "", "0",
            "--log-level=DEBUG"
        ])
    )
    env["GCP_PROJECT"] = "TEST_PROJECT"

    orig_env = dict(os.environ)
    try:
        os.environ.update(env)
        import_module("main")
    finally:
        os.environ.clear()
        os.environ.update(orig_env)


def check_url_in_cache(url):
    """Check whether the URL is sourced from storage or not."""
    url_encoded = quote(url)
    cache_redirector_url = os.environ["KCIDB_CACHE_REDIRECTOR_URL"]
    response = requests.get(
        f"{cache_redirector_url}?{url_encoded}",
        timeout=10,   # Time in secs
        allow_redirects=True
    )
    redirect_url = response.url
    if response.status_code == 200:
        # Check if the redirect URL matches the blob storage URL pattern
        if redirect_url.startswith('https://storage.googleapis.com/'):
            return True
    return False


def test_url_caching(empty_deployment):
    """kcidb cache client workflow test"""

    # Make empty_deployment appear used to silence pylint warning
    assert empty_deployment is None

    client = kcidb.Client(
        project_id=os.environ["GCP_PROJECT"],
        topic_name=os.environ["KCIDB_LOAD_QUEUE_TOPIC"]
    )

    data = {
        "version": {
            "major": 4,
            "minor": 0
        },
        "checkouts": [
            {
                "contacts": [
                    "rdma-dev-team@redhat.com"
                ],
                "start_time": "2020-03-02T15:16:15.790000+00:00",
                "git_repository_branch": "wip/jgg-for-next",
                "git_commit_hash": "5e29d1443c46b6ca70a4c940a67e8c09f05dcb7e",
                "patchset_hash": "",
                "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                "misc": {
                    "pipeline_id": 467715
                },
                "id": "non_test:1",
                "origin": "non_test",
                "patchset_files": [],
                "valid": True,
            },
            {
                "contacts": [
                    "rdma-dev-team@redhat.com"
                ],
                "start_time": "2020-03-02T15:16:15.790000+00:00",
                "git_repository_branch": "wip/jgg-for-next",
                "git_commit_hash": "1254e88b4fc1470d152f494c3590bb6a33ba0bab",
                "patchset_hash": "",
                "git_repository_url": "git://git.kernel.org/pub/scm/linux/kernel/git/rdma/rdma.git",
                "misc": {
                    "pipeline_id": 467715
                },
                "id": "test:1",
                "origin": "test",
                "patchset_files": [],
                "valid": True,
            },
        ],
        "builds": [
            {
                "architecture": "aarch64",
                "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                "config_name": "fedora",
                "duration": 237.0,
                "id": "non_test:1",
                "origin": "non_test",
                "input_files": [],
                "log_url": "https://github.com/kernelci/kcidb/blob/main/requirements.txt",
                "misc": {
                    "job_id": 678223,
                    "pipeline_id": 469720
                },
                "output_files": [],
                "checkout_id": "test:1",
                "start_time": "2020-03-03T17:52:02.370000+00:00",
                "valid": True
            },
            {
                "architecture": "aarch64",
                "command": "make -j30 INSTALL_MOD_STRIP=1 targz-pkg",
                "compiler": "aarch64-linux-gnu-gcc (GCC) 9.2.1 20190827 (Red Hat Cross 9.2.1-1)",
                "config_name": "fedora",
                "duration": 237.0,
                "id": "test:1",
                "origin": "test",
                "input_files": [],
                "log_url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/03/469720/build_aarch64.log",
                "misc": {
                    "job_id": 678223,
                    "pipeline_id": 469720
                },
                "output_files": [],
                "checkout_id": "test:1",
                "start_time": "2020-03-03T17:52:02.370000+00:00",
                "valid": True
            },
        ],
        "tests": [
            {
                "build_id": "non_test:1",
                "comment": "IOMMU boot test",
                "duration": 1847.0,
                "id": "non_test:1",
                "origin": "non_test",
                "output_files": [
                    {
                        "name": "x86_64_4_console.log",
                        "url": "https://github.com/kernelci/kcidb/blob/main/README.md"
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_dmesg.log",
                        "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_dmesg.log"
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_resultoutputfile.log",
                        "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_resultoutputfile.log"
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_taskout.log",
                        "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_taskout.log"
                    }
                ],
                "environment": {
                    "comment": "meson-gxl-s905d-p230 in lab-baylibre",
                    "misc": {
                        "device": "meson-gxl-s905d-p230",
                        "instance": "meson-gxl-s905d-p230-sea",
                        "lab": "lab-baylibre",
                        "mach": "amlogic",
                        "rootfs_url": "https://storage.kernelci.org/images/rootfs/buildroot/kci-2019.02-9-g25091c539382/arm64/baseline/rootfs.cpio.gz"
                    }
                },
                "path": "redhat_iommu_boot",
                "start_time": "2020-03-04T21:30:57+00:00",
                "status": "ERROR",
                "waived": True
            },
            {
                "build_id": "test:1",
                "comment": "IOMMU boot test",
                "duration": 1847.0,
                "id": "test:1",
                "origin": "test",
                "output_files": [
                    {
                        "name": "x86_64_4_console.log",
                        "url": "https://github.com/kernelci/kcidb/blob/main/setup.py"
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_dmesg.log",
                        "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_dmesg.log"
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_resultoutputfile.log",
                        "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_resultoutputfile.log"
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_taskout.log",
                        "url": "https://cki-artifacts.s3.amazonaws.com/datawarehouse/2020/03/04/471145/x86_64_4_IOMMU_boot_test_taskout.log"
                    }
                ],
                "environment": {
                    "comment": "meson-gxl-s905d-p230 in lab-baylibre",
                    "misc": {
                        "device": "meson-gxl-s905d-p230",
                        "instance": "meson-gxl-s905d-p230-sea",
                        "lab": "lab-baylibre",
                        "mach": "amlogic",
                        "rootfs_url": "https://storage.kernelci.org/images/rootfs/buildroot/kci-2019.02-9-g25091c539382/arm64/baseline/rootfs.cpio.gz"
                    }
                },
                "path": "redhat_iommu_boot",
                "start_time": "2020-03-04T21:30:57+00:00",
                "status": "ERROR",
                "waived": True
            },
        ],
    }
    
    # Submit data to submission queue
    client.submit(data)

    # Submit messages with different URLs
    urls_messages = [
        ["https://github.com/kernelci/kcidb/blob/main/setup.py"],
        ["https://github.com/kernelci/kcidb/blob/main/requirements.txt",
         "https://github.com/kernelci/kcidb/blob/main/README.md"]
    ]

    # Retry checking URLs in the cache for a minute
    retry_interval = 5  # seconds
    max_retries = 12  # 60 seconds / 5 seconds

    for urls in urls_messages:
        for url in urls:
            for _ in range(max_retries):
                if check_url_in_cache(url):
                    break
                time.sleep(retry_interval)
            else:
                raise AssertionError(f"URL '{url}' not found in the cache")
