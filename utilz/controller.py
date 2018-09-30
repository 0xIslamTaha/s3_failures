#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()

import click
from gevent.pool import Group
from jumpscale import j
from s3 import S3Manager


class Controller:
    def __init__(self, config):
        self.config = config
        j.clients.zrobot.get(self.config['robot']['client'], data={'url': config['robot']['url']})
        dm_robot = j.clients.zrobot.robots['controller']
        self.s3 = {}
        for service in dm_robot.services.find(template_name='s3'):
            self.s3[service.name] = S3Manager(self, service.name)

    def deploy_n(self, n, farm, size=20000, data=4, parity=2, login='admin', password='adminadmin'):
        start = len(self.s3)
        for i in range(start, start+n):
            name = 's3_controller_%d' % i
            self.s3[name] = S3Manager(self, name)
            self.s3[name].deploy(farm, size=size, data=data, parity=parity, login=login, password=password)

    def urls(self):
        return {name: url for name, url in self._do_on_all(lambda s3: (s3.name, s3.url))}

    def minio_config(self):
        return {name: config for name, config in self._do_on_all(lambda s3: (s3.name, s3.minio_config))}

    def states(self):
        return {name: config for name, config in self._do_on_all(lambda s3: (s3.name, s3.service.state))}

    def _do_on_all(self, func):
        group = Group()
        return group.imap_unordered(func, self.s3.values())


def read_config(path):
    config = j.data.serializer.yaml.load(path)
    return config


@click.command()
@click.option('--config', help='path to config file', default='controller.yaml')
def main(config):
    controller = controller(read_config('controller.yaml'))
    from IPython import embed
    embed()


# self.client.bash('test -b /dev/{0} && dd if=/dev/zero bs=1M count=500 of=/dev/{0}'.format(diskpath)).get()
if __name__ == '__main__':
    main()