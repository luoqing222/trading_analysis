__author__ = 'Qing'

import os
import zipfile
import configparser
import shelve
import datetime
import boto.glacier
import boto
from boto.glacier.exceptions import UnexpectedHTTPResponseError

ACCESS_KEY_ID = "AKIAIDD7XJVDDWL2EPWQ"
SECRET_ACCESS_KEY = "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"
REGION="us-west-1"
SHELVE_FILE = os.path.expanduser("~/.glaciervault.db")


class glacier_shelve(object):
    """
    Context manager for shelve
    """

    def __enter__(self):
        self.shelve = shelve.open(SHELVE_FILE)

        return self.shelve

    def __exit__(self, exc_type, exc_value, traceback):
        self.shelve.close()


class GlacierVault:
    """
    Wrapper for uploading/download archive to/from Amazon Glacier Vault
    Makes use of shelve to store archive id corresponding to filename and waiting jobs.

    Backup:>>> GlacierVault("myvault")upload("myfile")

    Restore:>>> GlacierVault("myvault")retrieve("myfile")

    or to wait until the job is ready:>>> GlacierVault("myvault")retrieve("serverhealth2.py", True)
    """

    def __init__(self, vault_name):
        """
        Initialize the vault
        """
        layer2 = boto.connect_glacier(aws_access_key_id=ACCESS_KEY_ID,
                                      aws_secret_access_key=SECRET_ACCESS_KEY,
                                      region_name=REGION)

        self.vault = layer2.get_vault(vault_name)

    def upload(self, filename):
        """
        Upload filename and store the archive id for future retrieval
        """
        archive_id = self.vault.create_archive_from_file(filename, description=filename)

        # Storing the filename => archive_id data.
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()

            archives = d["archives"]
            archives[filename] = archive_id
            d["archives"] = archives

    def get_archive_id(self, filename):
        """
        Get the archive_id corresponding to the filename
        """
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()

            archives = d["archives"]

            if filename in archives:
                return archives[filename]

        return None

    def retrieve(self, filename, wait_mode=False):
        """
        Initiate a Job, check its status, and download the archive when it's completed.
        """
        archive_id = self.get_archive_id(filename)
        if not archive_id:
            return

        with glacier_shelve() as d:
            if not d.has_key("jobs"):
                d["jobs"] = dict()

            jobs = d["jobs"]
            job = None

            if filename in jobs:
                # The job is already in shelve
                job_id = jobs[filename]
                try:
                    job = self.vault.get_job(job_id)
                except UnexpectedHTTPResponseError:  # Return a 404 if the job is no more available
                    pass

            if not job:
                # Job initialization
                job = self.vault.retrieve_archive(archive_id)
                jobs[filename] = job.id
                job_id = job.id

            # Commiting changes in shelve
            d["jobs"] = jobs

        print "Job {action}: {status_code} ({creation_date}/{completion_date})".format(**job.__dict__)

        # checking manually if job is completed every 10 secondes instead of using Amazon SNS
        if wait_mode:
            import time

            while 1:
                job = self.vault.get_job(job_id)
                if not job.completed:
                    time.sleep(10)
                else:
                    break

        if job.completed:
            print "Downloading..."
            job.download_to_file(filename)
        else:
            print "Not completed yet"


def zip_daily_data(src_directory, des_directory, zip_file_name):
    zf = zipfile.ZipFile(des_directory + "/" + zip_file_name, 'w')
    for dir_name, sub_dirs, files in os.walk(src_directory):
        zf.write(dir_name)
        for filename in files:
            zf.write(os.path.join(dir_name, filename))


if __name__ == "__main__":
    config_file = "option_data_management_setting.ini"
    Config = configparser.ConfigParser()
    Config.read(config_file)
    running_time = datetime.datetime.now()
    src_folder = Config.get("csv", "data_folder") + "/" + "daily_run" + "/" + running_time.strftime("%Y_%m_%d")
    des_folder = Config.get("csv", "data_folder") + "/" + "zip"
    if not os.path.exists(des_folder):
        os.makedirs(des_folder)
    zip_file_name = running_time.strftime("%Y_%m_%d") + ".zip"
    zip_daily_data(src_folder, des_folder, zip_file_name)

    vault_name = "Qing_Backup_Data"
    #print boto.ec2.regions()

    GlacierVault(vault_name).upload(des_folder + "/" + zip_file_name)
