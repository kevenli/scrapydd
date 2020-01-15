import sys
import os
from .runner import activate_egg


def main():
    egg_path = sys.argv[1]
    activate_egg(egg_path)
    print(os.environ['SCRAPY_SETTINGS_MODULE'])


if __name__ == '__main__':
    main()