from celery import Celery

app = Celery('pacs_tcp_client',
             broker='amqp://pacs_tcp_client:97OUWipH4txB@rabbitmq/it_support',
             backend='rpc://',
             include=['celery_config.tasks']
             )