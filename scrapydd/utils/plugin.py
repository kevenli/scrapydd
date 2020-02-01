import pkg_resources
import json
import sys
from six import next


def main(plugin_name):
    try:
        entry_point = next(pkg_resources.iter_entry_points('scrapydd_plugin', plugin_name))
    except StopIteration:
        sys.stderr.write('Cannot find plugin %s' % plugin_name)
        return sys.exit(1)
    settings = json.loads(input())
    plugin_execute = entry_point.load()
    output = plugin_execute(settings)
    print(output)


if __name__ == '__main__':
    main(sys.argv[1])
