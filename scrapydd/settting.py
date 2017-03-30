
from .models import Session, session_scope, SpiderSettings

class SpiderSettingLoader:
    def get_spider_setting(self, spider_id, setting_key):
        with session_scope() as session:
            setting = session.query(SpiderSettings).filter_by(spider_id=spider_id, setting_key=setting_key).first()
            return setting