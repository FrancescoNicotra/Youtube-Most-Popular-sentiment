# YouTap

to run this code, you need to install the following packages:

```
pip install argparse
pip install google-api-python-client
pip install python-dotenv
pip install requests

```

you have to download kafka.tgz from the following link: <a href="https://www.apache.org/dyn/closer.cgi?path=/kafka/3.4.0/kafka_2.13-3.4.0.tgz">kafka.tgz</a>

then, you have to save it in setup folder and run the following command:

```
docker build kafka/ -t kafka:latest
```

then, you have to run the following command:

```
docker build logstash/ -t logstash:latest
```

## How to run the code

```
dokcer-compose up
```

after every container is up and running, you have to run the following command:

```
python3 getData.py
```

Now on: http://localhost:9292/ you can see the dashboard and on topics section you will see your data
