from random import randint
from base_test import BaseTest


class TestS3Failures(BaseTest):
    def test001_stop_parity_zdb(self):
        """
        - upload 10M in setupClass
        - Stop n zdp, n <= parity
        - Downlaod 10M file, should succeed
        - assert md5 checksum is matching
        """
        import ipdb; ipdb.set_trace()
        self.logger.info(' [*] Stop {} zdb'.format((self.parity)))
        md5_before = self.file_name
        self.s3_controller.failures.zdb_down(count=self.parity)

        # input, output = self.s3_controller.perf.simple_write_read()
        # self.assertEqual(input, output.read(), ' [*] downloaded file != uploaded file')

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

