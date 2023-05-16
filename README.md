# YouTap

to run this code, you need to install the following packages:

```
pip install elasitcsearch
pip install __future__
pip install pandas
```

you have to download kafka.tgz from the following link: https://www.apache.org/dyn/closer.cgi?path=/kafka/3.4.0/kafka_2.13-3.4.0.tgz

then, you have to save it in setup folder and run the following command:

```
docker build kafka/ -t kafka
```

then, you have to run the following command:

```
docker build logstash/ -t logstash
```

then, you have to run the following command:

```
dokcer-compose up
```

## How to run the code

after every container is up and running, you have to run the following command:

```
python3 getData.py
```

Now on: http://localhost:9292/ you can see the dashboard and on topics section you will see your data
