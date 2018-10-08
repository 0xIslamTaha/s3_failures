from random import randint
from base_test import BaseTest


class TestS3Failures(BaseTest):

    def tearDown(self):
        super().tearDown()

    def test001_upload_stop_parity_zdb_download(self):
        """
        - upload 2M
        - Stop n zdb, n <= parity
        - Downlaod 10M file, should succeed
        - assert md5 checksum is matching
        - Start n zdb
        """
        self.file_name = self.upload_file()
        self.logger.info(' Stop {} zdb'.format((self.parity)))
        md5_before = self.file_name
        self.s3.failures.zdb_down(count=self.parity)

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info(' Start {} zdb'.format((self.parity)))
        self.s3.failures.zdb_up(count=self.parity)

    def test002_stop_parity_zdb_upload_start_download(self):
        """
        - Stop n zdb, n <= parity
        - upload file, should pass
        - start n zdb, should pass
        - download file, should pass
        - assert md5 checksum is matching
        """
        self.file_name = self.upload_file()
        self.logger.info(' Stop {} zdb'.format((self.parity)))
        md5_before = self.file_name
        self.s3.failures.zdb_down(count=self.parity)

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info(' Start {} zdb'.format((self.parity)))
        self.s3.failures.zdb_up(count=self.parity)

