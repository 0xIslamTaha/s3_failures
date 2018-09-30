from unittest import TestCase
from utilz.controller import Controller
from uuid import uuid4
import time
logger = j.logger.get('s3_failures')


class BaseTest(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = j.data.serializer.yaml.load('./config.yaml')
        self.logger = logger

    def setUpClass(cls):
        """
        Deploy s3.

        function to deploy s3 with one of pre-configured parameters.

        """
        s3_service_name = str(uuid4()).replace('-', '')[:10]
        logger.info(" [*] s3 service name : {}".format(s3_service_name))
        config = j.data.serializer.yaml.load('./config.yaml')

        cls.s3_controller = Controller(config, s3_service_name)
        cls.shards = 4
        cls.parity = 2
        cls.s3_controller.s3.deploy(config['farmer']['name'], 10000, cls.shards, cls.parity, 2000)

        while True:
            state = cls.s3_controller.s3.service.state
            logger.info(" s3 state : {}".format(state))

            if not state:  # TODO
                time.sleep(5 * 60)
                logger.info(" [*] wait for 5 mins")
            else:
                logger.info(" [*] Hope s3 is working! ")
                break

    def tearDownClass(cls):
        """
        TearDown s3 instance

        :return:
        """
        logger.info(" [*] Delete s3 instance")
        cls.s3_controller.s3.remove()
