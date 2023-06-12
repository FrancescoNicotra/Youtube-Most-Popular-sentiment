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
docker build kafka/ -t youtap:kafka
```

then, you have to run the following command:

```
docker build logstash/ -t youtap:logstash
```

## How to run the code

If you want to use Jupyter, you can run this command:

```
jupyter notebook presentazione.ipynb
```

and then follow the steps of the presentation.

## Alternatively, you can execute the following commands:

```
dokcer-compose up
```

or

```
docker-compose up -d
```

after every container is up and running, you have to run the following command:

```
python3 getData.py
```

Once the script has finished, you can go to the following link to view the graphs on Kibana: <a href="http://localhost:5601/app/dashboards#/view/c2e910b0-0903-11ee-8d24-1b3026e98ad5?_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))"> Dashboard Kibana </a>

## How to stop the code

```
docker-compose down
```
