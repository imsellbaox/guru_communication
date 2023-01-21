import pika
import configparser

# 生产者
class dataCache:
    def __init__(self):
        cf = configparser.ConfigParser()
        cf.read("./config.ini")
        user = cf.get("rabbitMQ", "user")
        password = cf.get("rabbitMQ", "password")
        host = cf.get("rabbitMQ", "host")
        port = int(cf.get("rabbitMQ", "port"))

        # 建立channel通道
        credentials = pika.PlainCredentials(user, password)
        cpara = pika.ConnectionParameters(host=host, port=port, credentials=credentials)
        connection = pika.BlockingConnection(cpara)
        self.channel = connection.channel()


    def send_channel(self,que_name:str,routing_key:str,body:str):
        self.channel.queue_declare(queue=que_name,durable=False)
        self.channel.basic_publish(exchange='',routing_key=routing_key,body=body)
    def close_channel(self):
        self.channel.close()



# 消费者
class cacheFunc:
    def __init__(self):
        cf = configparser.ConfigParser()
        cf.read("./config.ini")
        user = cf.get("rabbitMQ", "user")
        password = cf.get("rabbitMQ", "password")
        host = cf.get("rabbitMQ", "host")
        port = int(cf.get("rabbitMQ", "port"))

        # 建立channel通道
        credentials = pika.PlainCredentials(user, password)
        cpara = pika.ConnectionParameters(host=host, port=port, credentials=credentials)
        connection = pika.BlockingConnection(cpara)
        self.channel = connection.channel()
    def channel_do_func(self,que_name,func, **kwargs):
        self.channel.queue_declare(queue=que_name,durable=False)
        def callback(ch, method, propeties, body):
            kwargs["data"]=body.decode()
            func(**kwargs)
            ch.basic_ack(
                delivery_tag=method.delivery_tag)  # 由消费者告诉rabbitmq处理完消息可以删除了，如果当前consumer挂掉了，重新递交给其他的consumer，如果有的话

        self.channel.basic_consume(queue=que_name, on_message_callback=callback)
        self.channel.start_consuming()

