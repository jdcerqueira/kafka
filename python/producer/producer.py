from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['192.168.15.5:9092'])
producer.send('ECOMMERCE_NEW_ORDER', key=b'python2:1234,1233,15454', value=b'valores e valores').get()
print('Mensagem enviada')