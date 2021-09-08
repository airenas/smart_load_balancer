import subprocess
from pathlib import Path

from setuptools import setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
git_version = subprocess.run(["git", "rev-list", "--count", "HEAD"], stdout=subprocess.PIPE).stdout.decode(
    'utf-8').strip()

setup(
    name='smart_load_balancer',
    url='https://github.com/airenas/smart_load_balancer',
    author='Airenas Vaičiūnas',
    author_email='airenass@gmail.com',
    packages=['smart_load_balancer'],
    # install_requires=['numpy'],
    version='0.1.' + git_version,
    license='BSD-3',
    description='Stateful workers load balancer',
    long_description=long_description,
    long_description_content_type='text/markdown',
    python_requires=">=3.6"
)
