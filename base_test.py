from unittest import TestCase
from utilz.controller import Controller
from uuid import uuid4
from jumpscale import j
from subprocess import Popen, PIPE
from minio import Minio
import time, os, hashlib

logger = j.logger.get('s3_failures')


class BaseTest(TestCase):
    file_name = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = j.data.serializer.yaml.load('./config.yaml')
        self.logger = logger

    @classmethod
    def setUpClass(cls):
        """
        Deploy s3.

        function to deploy s3 with one of pre-configured parameters.

        """
        self = cls()
        config = j.data.serializer.yaml.load('./config.yaml')
        if config['s3']['deploy']:
            cls.s3_controller = Controller(config)
            s3_service_name = str(time.time()).split('.')[0]
            logger.info("s3 service name : {}".format(s3_service_name))

            instance = cls.s3_controller.deploy(s3_service_name, config['s3']['instance']['farm'],
                                                config['s3']['instance']['size'],
                                                config['s3']['instance']['shards'],
                                                config['s3']['instance']['parity'],
                                                config['s3']['instance']['shard_size'])
            logger.info("wait for deploying {} s3 service".format(s3_service_name))
            instance.wait(die=True)
            for _ in range(50):
                cls.s3 = cls.s3_controller.s3[s3_service_name]
                state = cls.s3.service.state
                logger.info(" s3 state : {}".format(state))
                try:
                    state.check('actions', 'install', 'ok')
                    logger.info(" waiting s3 state to be ok ... ")
                    break
                except:
                    time.sleep(5 * 60)
                    logger.info("wait for 5 mins")
        else:
            sub = Popen('zrobot godtoken get', stdout=PIPE, stderr=PIPE, shell=True)
            out, err = sub.communicate()
            god_token = str(out).split(' ')[2]
            cls.s3_controller = Controller(config, god_token)
            cls.s3_service_name = config['s3']['use']['s3_service_name']
            if cls.s3_service_name not in cls.s3_controller.s3:
                logger.error("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                    config['robot']['client']))
                raise Exception("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                config['robot']['client']))
        cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
        self.get_s3_info()
        cls.file_name = self.upload_file()

    @classmethod
    def tearDownClass(cls):
        """
        TearDown

        :return:
        """
        if not cls.s3_service_name:
            logger.info("Delete s3 instance")
            cls.s3_controller.s3.remove()

    def setUp(self):
        self.s3 = self.s3_controller.s3[self.s3_service_name]
        self.get_s3_info()

    def upload_file(self):
        """
         - Create random 10M file
         - Calc its md5 checksum hash
         - Rename file to make its name = md5
         - Upload it
        :return: file_name
        """
        with open('%s' % 'random', 'wb') as fout:
            fout.write(os.urandom(1024 * 1024 * 10))  # 1

        self.file_name = self.calc_md5_checksum('random')

        os.rename('random', self.file_name)

        import ipdb; ipdb.set_trace()
        self.logger.info('config s3Minio')
        config_minio_cmd = '/bin/mc config host add s3Minio {} {} {}'.format(self.minio['minio_ip'],
                                                                             self.minio['username'],
                                                                             self.minio['password'])
        out, err = self.execute_cmd(cmd=config_minio_cmd)
        if err:
            self.logger.error(err)

        self.logger.info('create TestingBucket bucket')
        creat_bucket_cmd = '/bin/mc mb s3Minio/{}'.format('TestingBucket')
        self.execute_cmd(cmd=creat_bucket_cmd)
        if err:
            self.logger.error(err)

        upload_cmd = '/bin/mc cp {} s3Minio/TestingBucket/{}'.format(self.file_name, self.file_name)
        out, err = self.execute_cmd(cmd=upload_cmd)
        if err:
            self.logger.error(err)

        return self.file_name

    def download_file(self, file_name):
        """
         - downlaod file
         - return its md5 checksum hash
        :return: str(downloaded_file_md5)
        """
        upload_cmd = '/bin/mc cp s3Minio/TestingBucket/{} {}_out'.format(file_name, file_name)
        out, err = self.execute_cmd(cmd=upload_cmd)
        self.logger.info(out)
        self.logger.err(err)
        return self.calc_md5_checksum('{}_out'.format(file_name))

    def get_s3_info(self):
        self.s3_data = self.s3.service.data['data']
        self.parity = self.s3_data['parityShards']
        self.shards = self.s3_data['dataShards']

        self.minio = {'minio_ip': self.s3_data['minioUrls']['public'],
                      'username': self.s3_data['minioLogin'],
                      'password': self.s3_data['minioPassword']
                      }

    def execute_cmd(self, cmd):
        sub = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        out, err = sub.communicate()
        return out, err

    def calc_md5_checksum(self, file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
