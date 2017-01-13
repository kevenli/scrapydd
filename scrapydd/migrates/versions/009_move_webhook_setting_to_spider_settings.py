from sqlalchemy import *
from migrate import *

meta = MetaData()

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    spider_webhook = Table('spider_webhook', meta, autoload=True)
    spider_settings = Table('spider_settings', meta, autoload=True)

    for spider_webhook_row in migrate_engine.execute(spider_webhook.select()).fetchall():
        spider_id = spider_webhook_row.id
        webhook_payload = spider_webhook_row.payload_url

        migrate_engine.execute(spider_settings.insert().values(spider_id=spider_id, setting_key='webhook_payload', value=webhook_payload))


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pass
