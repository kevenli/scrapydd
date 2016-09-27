
def list_spiders():
    project = txrequest.args['project'][0]
    version = txrequest.args.get('_version', [''])[0]
    spiders = get_spider_list(project, runner=self.root.runner, version=version)
    return spiders