from ..models import User, session_scope, init_database
from ..config import Config
from getpass import getpass
from ..security import encrypt_password
from six.moves import input

class ResetPasswordCommand():
    def run(self):
        config = Config()
        init_database(config)
        print('Please input username:')
        username = input()

        print('Plase input new password:')
        password = getpass()

        with session_scope() as session:
            user = session.query(User).filter_by(username=username).first()
            if not user:
                print('User does not exist.')
                return

            secret_key = config.get('secret_key')
            encrypted_password = encrypt_password(password, secret_key)
            user.password = encrypted_password
            session.add(user)
            session.commit()

        print('User password updated.')