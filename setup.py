from setuptools import find_packages, setup

package_name = "iii_drone_runtime"

setup(
    name=package_name,
    version="0.1.0",
    packages=find_packages(exclude=["test"]),
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=[
        "setuptools",
        "pydantic>=2,<3",
        "fastapi>=0.110,<1",
        "httpx>=0.27,<1",
        "PyYAML>=6,<7",
        "uvicorn>=0.29,<1",
        "websockets>=12,<16",
        "zeroconf>=0.132,<1",
    ],
    tests_require=["pytest"],
    zip_safe=True,
    maintainer="ffn",
    maintainer_email="ffn@sdu.dk",
    description="Runtime-host daemon transport and iii-runtime-api service for III-Drone.",
    license="Proprietary",
    entry_points={
        "console_scripts": [
            "iii-runtime-api = iii_drone_runtime.api.main:main",
        ],
    },
)
