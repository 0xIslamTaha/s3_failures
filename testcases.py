from random import randint
from base_test import BaseTest


class TestS3Failures(BaseTest):
    def test001_stop_parity_zdb(self):
        """
        Stop n zdp, n <= parity. S3 shoule still working fine.

        :return:
        """
        import ipdb; ipdb.set_trace()
        no_of_zdb = randint(1, self.parity)
        self.logger.info(' [*] Stop {} zdb'.format(no_of_zdb))
        self.s3_controller.failures.zdb_down(count=no_of_zdb)

        input, output = self.s3_controller.perf.simple_write_read()
        self.assertEqual(input, output.read(), ' [*] downloaded file != uploaded file')
