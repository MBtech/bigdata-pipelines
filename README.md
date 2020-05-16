# Bigdata Demo pipelines
This reporsitory contains demo pipelines for bigdata analysis using Apache Spark

## Installation
<!-- - Install homebrew `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"` -->
- Install home brew:
    ```bash
    sudo apt update
    sudo apt install linuxbrew-wrapper
    ```
- Install Spark, Scala, and awscli: `brew install spark scala awscli`
- Install pip: `sudo apt instlal python3-pip`
- Clone the repository: `git clone https://github.com/MBtech/bigdata-pipelines.git`
- Install python modules: `cd bigdata-pipelines/; pip3 install -r requirements.txt`

## Setup
- Modify the `config_template.json` in `housing-prices` directory to create `config.json` with appropriate configuration values.
- If you are running this in EC2 you can assign a proper role to your EC2 instance so that it can access S3 and in that case there is no need to configure the API keys in `config.json`

