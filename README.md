# YouTap

to run this code, you need to install the following packages:

```
pip install elasitcsearch
pip install __future__
pip install requests
pip install json
pip install argparse
pip install InstalledAppFlow
pip install google_auth_oauthlib
```

you have to download kafka.tgz from the following link: https://www.apache.org/dyn/closer.cgi?path=/kafka/3.4.0/kafka_2.13-3.4.0.tgz

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
