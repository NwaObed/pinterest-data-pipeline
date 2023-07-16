# Pinterest Data Pipeline

### Requirements
- AWS account
- EC2 Instance
- Kinesis Console
- S3


### Set up the virtual environment

```python
git clone https://github.com/NwaObed/pinterest-data-pipeline.git

conda env create -f pinterest_env.yaml -n <new-env>
```
This will emulate the virtual environment from the `pinterest_env.yaml` file and install the `dependencies.`

### Connect to your EC2
```ssh -i <key-pair-name> ec2-user@<public-dns> ```

### Start your EC2
```
cd confluent-7.2.0/bin/

./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties
```
### Start Posting data to Kafka topics
- For batch data
```
python3 python3 user_posting_emulation.py
```
If you go to your S3, you should start seeing the data arriving

- For streaming data
```
python3 user_posting_emulation_streaming.py
```
If you go to Kinesis console, you should see the streaming data arriving.

### Upload notebook to databricks
Go on to your databricks account:
- Click on file
- Click on `Import Notebook`
- Drag and drop the notebook or browse to the location
- Click on `Import`