from unittest import TestCase
from utilz.controller import Controller
from uuid import uuid4
from jumpscale import j
import time

logger = j.logger.get('s3_failures')


class BaseTest(TestCase):
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
        config = j.data.serializer.yaml.load('./config.yaml')
        cls.s3_controller = Controller(config)

        if config['s3']['deploy']:
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
            cls.s3_service_name = config['s3']['use']['s3_service_name']
            if cls.s3_service_name not in cls.s3_controller.s3:
                logger.error("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                    config['robot']['client']))
                raise Exception("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                       config['robot']['client']))

    @classmethod
    def tearDownClass(cls):
        """
        TearDown s3 instance

        :return:
        """
        if not cls.s3_service_name:
            logger.info("Delete s3 instance")
            cls.s3_controller.s3.remove()
