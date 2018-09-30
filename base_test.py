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
            s3_service_name = str(uuid4()).replace('-', '')[:10]
            logger.info(" [*] s3 service name : {}".format(s3_service_name))
            cls.s3_controller.s3.deploy_n(1, config['s3']['farm'], config['s3']['size'], config['s3']['shards'],
                                          config['s3']['parity'], config['s3']['shard_size'])

            while True:
                state = cls.s3_controller.s3.service.state
                logger.info(" s3 state : {}".format(state))

                if not state:  # TODO
                    time.sleep(5 * 60)
                    logger.info(" [*] wait for 5 mins")
                else:
                    logger.info(" [*] Hope s3 is working! ")
                    break
        else:
            s3_service_name = config['s3']['s3_service_name']
            if s3_service_name not in cls.s3_controller.s3:
                logger.error(" [*] cant find {} s3 service under {} robot client".format(s3_service_name,
                                                                                         config['robot']['client']))
                raise Exception(" [*] cant find {} s3 service under {} robot client".format(s3_service_name,
                                                                                            config['robot']['client']))
    @classmethod
    def tearDownClass(cls):
        """
        TearDown s3 instance

        :return:
        """
        logger.info(" [*] Delete s3 instance")
        cls.s3_controller.s3.remove()
