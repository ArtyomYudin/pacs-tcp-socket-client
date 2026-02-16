from celery import Celery

from core.settings import settings

app = Celery('pacs_tcp_client',
             broker=settings.CELERY_BROKER_URL,
             backend=settings.CELERY_RESULT_BACKEND,
             include=['celery_config.tasks']
             )