from ..models import User, session_scope, init_database
from ..config import Config
import hashlib
from getpass import getpass

class ResetPasswordCommand():
    def run(self):
        config = Config()
        init_database(config)
        print('Please input username:')
        username = raw_input()

        print('Plase input new password:')
        password = getpass()

        with session_scope() as session:
            user = session.query(User).filter_by(username=username).first()
            if not user:
                print('User does not exist.')
                return

            m = hashlib.md5()
            m.update(password)
            encrypted_password = m.hexdigest()
            user.password = encrypted_password
            session.add(user)
            session.commit()

        print('User password updated.')