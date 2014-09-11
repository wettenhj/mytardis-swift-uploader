# MyTardis / RDSI upload script, used for IonTorrent next-gen seq data.
# MyTardis Swift Uploader v1
# Author: James Wettenhall <james.wettenhall@monash.edu>
# Based on MyTardis Uploader v1.1
# by Steve Androulakis <steve.androulakis@monash.edu>
# Thanks Grischa Meyer <grischa.meyer@monash.edu> for initial script

# This version uses the directory field within the Datafile model,
# in some queries' WHERE clauses, so if you don't already have an
# index for the directory field, you should create one.

# To do:
# 1. Script currently queries MyTardis to see if a datafile already
#      exists before uploading.  But if we know the dataset ID, we
#      can also query the underlying storage in this case (Swift),
#      e.g. swift stat next_gen_seq "dataset_name-dataset_id/subdir/filename"
#      and then, if it exists, just register it via the API, instead of
#      uploading it via POST.  That way, if the upload script gets stuck
#      POSTing a large file, we can manually upload it using swiftclient.
# 2. Can we simplify the way the storage box is specified, e.g.
#      just pass in a string, like in the
#      "Via shared permanent storage location" example here:
#      https://mytardis.readthedocs.org/en/3.5/api.html

import base64
import os
import os.path
import mimetypes
import json
import requests
from requests.auth import HTTPBasicAuth
from time import strftime
import csv
import poster
import io
import urllib2
import sys
import openrc
import subprocess

mytardis_base_url = "http://115.146.93.73"
mytardis_username = "hilllab"
mytardis_apikey = "0c0e8965199b5648f5dc7f62d76c31f38922bb5d"

swift_container_name = "next_gen_seq"

USE_CHECKSUMS_TO_DETERMINE_FILES_ALREADY_UPLOADED = True


class MyTardisSwiftUploader:

    def __init__(self,
                 mytardis_url,
                 username,
                 apikey,
                 ):

        self.mytardis_url = mytardis_url
        self.v1_api_url = mytardis_url + "/api/v1/%s"
        self.username = username
        self.apikey = apikey

    def upload_directory(self,
                         file_path,
                         title='',
                         institute='',
                         description='',
                         test_run=False):

        # First, let's check whether a storage box exists for "swift":

        storage_box_name = "swift"
        url = self.mytardis_url + "/api/v1/location/?format=json&name=" + \
            storage_box_name
        headers = {'Authorization': 'ApiKey ' +
                   self.username + ":" + mytardis_apikey}
        response = requests.get(url=url, headers=headers)
        num_storage_boxes_found = 0
        storage_boxes_json = None
        try:
            storage_boxes_json = response.json()
            num_storage_boxes_found = storage_boxes_json['meta']['total_count']
        except:
            print str(url)
            print str(response.text)
            print str("response.status_code = " + str(response.status_code))
            return None

        if num_storage_boxes_found == 0:
            print "ERROR: No storage box found for swift.\n"
            print str(url)
            print str(response.text)
            print str("response.status_code = " + str(response.status_code))
            return None

        storage_boxes_list = [storage_boxes_json['objects'][0]['resource_uri']]

        exp_title = title or os.path.basename(os.path.abspath(file_path))

        created = False
        exp_url = self.get_existing_experiment(exp_title)

        # print exp_url
        if exp_url is None:
            print "Creating experiment: %s" % exp_title

            created = False
            exp_url = "/test/"
            description = \
                ("%s \n\n" +
                 "Automatically generated on %s by " +
                 "https://github.com/wettenhj/mytardis-swift-uploader") \
                % (description or "No description.",
                   strftime("%Y-%m-%d %H:%M:%S"))
            if not test_run:
                exp_url = self.create_experiment(exp_title,
                                                 institute or "",
                                                 description)
                created = True
        else:
            print "Found existing experiment for %s" % exp_title
            # print exp_url

        exp_id = exp_url.rsplit('/')[-2]

        ds_desc = exp_title

        # print "Checking whether dataset already exists for: %s" % ds_desc
        ds_url = self.get_existing_dataset(ds_desc, exp_id)

        if ds_url is None:
            print '\tCreating dataset: %s' % ds_desc

            ds_url = "/test/"
            if not test_run:
                # print "exp_url = " + exp_url
                description = str(ds_desc)
                experiments_list = [self._get_path_from_url(exp_url)]
                ds_url = \
                    self.create_dataset(description,
                                        storage_boxes_list,
                                        experiments_list)
        else:
            print "\tFound existing dataset for %s" % ds_desc
            # print ds_url

        if not USE_CHECKSUMS_TO_DETERMINE_FILES_ALREADY_UPLOADED:
            print "\t\tWARNING: Not using checksums to determine " + \
                "which files have already been uploaded."
        for dirname, dirnames, filenames in os.walk(file_path):

            for filename in filenames:

                # print "filename = " + filename

                if filename.startswith('.'):
                    continue  # filter files/dirs starting with .

                sub_file_path = os.path.join(dirname, filename)

                subdirectory = os.path.relpath(dirname, file_path)
                if subdirectory == ".":
                    subdirectory = ""

                dataset_id = ds_url.rsplit('/')[-2]
                md5sum = None
                if USE_CHECKSUMS_TO_DETERMINE_FILES_ALREADY_UPLOADED:
                    md5sum = self._md5_file_calc(sub_file_path)
                size = os.path.getsize(sub_file_path)
                datafile_exists_in_mytardis = \
                    self.datafile_exists_in_mytardis(filename, subdirectory,
                                                     dataset_id, md5sum)

                datafile_exists_in_swift = False
                if not datafile_exists_in_mytardis:
                    datafile_exists_in_swift = \
                        self.datafile_exists_in_swift(filename, subdirectory,
                                                      dataset_id, ds_desc,
                                                      md5sum)
                if datafile_exists_in_swift and \
                        not datafile_exists_in_mytardis:
                    print "Datafile doesn't exist in MyTardis, but it was " + \
                        "found in Swift, so maybe we can just register " + \
                        "it, without needing to upload it using HTTP POST."
                elif not datafile_exists_in_mytardis and \
                        not datafile_exists_in_swift:
                    print "\t\tDidn't find existing datafile in MyTardis: %s" \
                        % filename
                    print "\t\tUploading file '%s' to dataset '%s'." % \
                        (sub_file_path, ds_desc)

                    if md5sum is None:
                        md5sum = self._md5_file_calc(sub_file_path)
                    # f_url = "/test/"
                    if not test_run:
                        dataset_path = self._get_path_from_url(ds_url)
                        location = self.upload_file(sub_file_path,
                                                    subdirectory,
                                                    dataset_path,
                                                    md5sum, size)
                        fail_count = 0
                        while location is None:

                            fail_count = fail_count + 1
                            if fail_count >= 10:
                                print "\t\tBailing out " \
                                      "after 10 failed uploads."
                                sys.exit(1)
                            print "\t\tRetrying failed upload for: " \
                                + sub_file_path

                            dataset_path = self._get_path_from_url(ds_url)
                            location = self.upload_file(sub_file_path,
                                                        subdirectory,
                                                        dataset_path,
                                                        md5sum, size)
                        # print f_url
                else:
                    print "\t\t%s has already been uploaded." \
                        % os.path.join(dirname, filename)

        if created:
            exp_id = exp_url.rsplit('/')[-2]
            new_exp_url = "%s/experiment/view/%s/" \
                % (self.mytardis_url, exp_id)
            print "Experiment created: %s\n" % new_exp_url
            return new_exp_url

        else:
            if test_run:
                print "Dry run complete.\n"
                return "http://example.com/test/success"
            else:
                print ""
                return None

    def _send_data(self, data, urlend, method="POST"):
        url = self.v1_api_url % urlend
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        headers['Authorization'] = "ApiKey " + \
            self.username + ":" + mytardis_apikey

        response = "error"
        if method == "POST":
            response = requests.post(headers=headers, url=url, data=data)
        elif method == "PUT":
            response = requests.put(headers=headers, url=url, data=data)
        else:
            print "Assuming method is GET.  Actual method is " + method
            response = requests.get(headers=headers, url=url, data=data)

        if response.status_code < 200 or response.status_code >= 300:
            print "HTTP status code: " + str(response.status_code)
            print str(url)
            print str(data)
            print str(headers)
            print response.text
        return response

    def _md5_file_calc(self, file_path):
        import hashlib
        return hashlib.md5(open(file_path, 'rb').read()).hexdigest()

    def _send_datafile(self, data, urlend, method='POST', file_path=None):
        url = self.v1_api_url % urlend

        # print "_send_datafile: url = " + url
        # print "_send_datafile: data = " + data

        datafileBufferedReader = io.open(file_path, 'rb')
        # poster allows a callback for updating a progress bar (not used here).
        datagen, headers = poster.encode\
            .multipart_encode({"json_data": data,
                               "attached_file": datafileBufferedReader})

        opener = poster.streaminghttp.register_openers()

        opener.addheaders = [('Authorization', 'ApiKey ' +
                              self.username + ':' +
                              mytardis_apikey)]

        req = urllib2.Request(url, datagen, headers)
        # req.add_header('Accept', 'application/json')
        # req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', u'ApiKey ' +
                       self.username + ':' +
                       mytardis_apikey)
        try:
            response = urllib2.urlopen(req)
        except:
            print "\n" + str(req.header_items()) + "\n"
            # raise
            return None

        return response

    def _get_header(self, headers, key):

        import string

        location = None
        for header in string.split(headers, '\n'):
            if header.startswith('%s: ' % key):
                location = string.split(header, '%s: ' % key)[1].strip()
                break

        return location

    def _get_path_from_url(self, url_string):

        from urlparse import urlparse

        o = urlparse(url_string)

        return o.path

    def get_existing_experiment(self, title):

        url = self.mytardis_url + "/api/v1/experiment/?format=json" + \
            "&title=" + urllib2.quote(title)
        headers = {"Authorization": "ApiKey " +
                   self.username + ":" + mytardis_apikey}
        response = requests.get(url=url, headers=headers)
        num_experiments_found = 0
        experiments_json = None
        try:
            experiments_json = response.json()
            num_experiments_found = experiments_json['meta']['total_count']
        except:
            print str(url)
            print str(response.text)
            print str("response.status_code = " + str(response.status_code))
            return None

        if num_experiments_found == 0:
            return None

        return experiments_json['objects'][0]['resource_uri']

    def create_experiment(self, title, institution, description):

        exp_dict = {
            u'description': description,
            u'institution_name': institution,
            u'title': title,
            u'immutable': False
            }

        exp_json = json.dumps(exp_dict)

        data = self._send_data(exp_json, 'experiment/')
        return data.headers['Location']

    def get_existing_dataset(self, description, exp_id):

        url = self.mytardis_url + "/api/v1/dataset/?format=json" + \
            "&experiments__id=" + exp_id + \
            "&description=" + urllib2.quote(description)
        headers = {'Authorization': 'ApiKey ' +
                   self.username + ":" + mytardis_apikey}
        response = requests.get(url=url, headers=headers)
        num_datasets_found = 0
        datasets_json = None
        try:
            datasets_json = response.json()
            num_datasets_found = datasets_json['meta']['total_count']
        except:
            print str(url)
            print str(response.text)
            print str("response.status_code = " + str(response.status_code))
            return None

        if num_datasets_found == 0:
            return None

        return datasets_json['objects'][0]['resource_uri']

    def create_dataset(self, description, storage_boxes_list,
                       experiments_list, immutable=False):

        dataset_dict = {u'description': description,
                        u'storage_boxes': storage_boxes_list,
                        u'experiments': experiments_list,
                        u'parameter_sets': [],
                        u'immutable': immutable}

        dataset_json = json.dumps(dataset_dict)

        # print "\ndataset_json = " + dataset_json + "\n"

        data = self._send_data(dataset_json, 'dataset/')

        # print str(data.text)
        # print "#####"
        # print str(data.status_code)
        # print "#####"
        # print str(data.headers)
        return data.headers['Location']

    def datafile_exists_in_mytardis(self, filename, subdirectory,
                                    dataset_id, md5sum):

        url = self.mytardis_url + "/api/v1/dataset_file/?format=json" + \
            "&dataset__id=" + dataset_id + \
            "&filename=" + urllib2.quote(filename) + \
            "&directory=" + urllib2.quote(subdirectory)

        if md5sum is not None:
            url = url + \
                "&md5sum=" + urllib2.quote(md5sum)

        headers = {'Authorization': 'ApiKey ' +
                   self.username + ":" + mytardis_apikey}
        response = requests.get(url=url, headers=headers)
        num_datafiles_found = 0
        datafiles_json = None
        try:
            datafiles_json = response.json()
            num_datafiles_found = datafiles_json['meta']['total_count']
        except:
            print str(url)
            print str(response.text)
            print str("response.status_code = " + str(response.status_code))
            return False

        # print str(url)
        # print str(response.text)
        # print str("response.status_code = " + str(response.status_code))
        # print str(datafiles_json)

        if num_datafiles_found == 0:
            return False

        return True

    def datafile_exists_in_swift(self, filename, subdirectory,
                                 dataset_id, dataset_name, md5sum):
        print "\t\tChecking whether datafile exists in Swift:"
        swift_object_name = dataset_name + "-" + dataset_id + "/" + \
            os.path.join(subdirectory, filename)
        # print "\t\tswift_object_name: " + swift_object_name
        swift_stat_cmd = "swift stat \"%s\" \"%s\"" % (swift_container_name,
                                                       swift_object_name)
        print "\t\tswift_stat_cmd: " + str(swift_stat_cmd)
        swift_stat_proc = subprocess.Popen(swift_stat_cmd,
                                           stdout=subprocess.PIPE,
                                           stdin=subprocess.PIPE,
                                           stderr=subprocess.PIPE,
                                           shell=True, universal_newlines=True)
        stdout, stderr = swift_stat_proc.communicate()
        lines = stdout.split("\n")
        object_found = False
        for line in lines:
            if line.strip().startswith("ETag: "):
                object_found = True
                swift_object_md5sum = \
                    int(line.strip().split("ETag: ")[1])
                break
        if object_found:
            print "\t\t%s was found in Swift." % swift_object_name
            print "\t\tFile md5sum: %s." % swift_object_md5sum
            return True

        return False

    def upload_file(self, file_path, subdirectory,
                    dataset_path, md5sum, size):

        file_dict = {u'dataset': dataset_path,
                     u'filename': os.path.basename(file_path),
                     u'directory': subdirectory,
                     u'md5sum': md5sum,
                     u'mimetype': mimetypes.guess_type(file_path)[0],
                     u'size': size,
                     u'parameter_sets': []}

        file_json = json.dumps(file_dict)
        data = self._send_datafile(file_json, 'dataset_file/', 'POST',
                                   file_path)
        if data is None:
            return None

        return data.headers['location']


def run():
    ####
    # Le Script
    ####
    #   steve.androulakis@monash.edu
    ####

    from optparse import OptionParser
    import getpass

    print ""
    print "MyTardis Swift Uploader v1"
    print "James Wettenhall <james.wettenhall@monash.edu>"
    print ""
    print "Based on MyTardis uploader generic v1.1"
    print "Steve Androulakis <steve.androulakis@monash.edu>"
    print ""
    print "Uploads the given directory as a Dataset, stored within a " + \
        "MyTardis Experiment of the same name,"
    print ""
    print "e.g. python mytardis_swift_uploader.py -f /results/sn11c080309/" + \
        "R_2014_06_10_06_19_13_user_SN1-175-Liz_4"
    print ""

    parser = OptionParser()
    parser.add_option("-f", "--path", dest="file_path",
                      help="The PATH of the experiment to be uploaded",
                      metavar="PATH")
    parser.add_option("-l", "--url", dest="mytardis_url",
                      help="The URL to the MyTardis installation",
                      metavar="URL")
    parser.add_option("-u", "--username", dest="username",
                      help="Your MyTardis USERNAME", metavar="USERNAME")
    parser.add_option("-k", "--apikey", dest="apikey",
                      help="Your MyTardis API key", metavar="APIKEY")
    parser.add_option("-t", "--title", dest="title",
                      help="Experiment TITLE", metavar="TITLE")
    parser.add_option("-d", "--description", dest="description",
                      help="Experiment DESCRIPTION", metavar="DESCRIPTION")
    parser.add_option("-i", "--institute", dest="institute",
                      help="Experiment INSTITUTE (eg university)",
                      metavar="INSTITUTE")
    parser.add_option("-r", "--dry",
                      action="store_true", dest="dry_run", default=False,
                      help="Dry run (don't create anything)")

    (options, args) = parser.parse_args()

    if not options.file_path:
        parser.error('file path not given')

    if not options.mytardis_url:
        # parser.error('url to MyTardis not given')
        options.mytardis_url = mytardis_base_url

    if not options.username:
        # parser.error('MyTardis username not given')
        options.username = mytardis_username

    if not options.apikey:
        # parser.error('MyTardis API key not given')
        options.apikey = mytardis_apikey

    file_path = options.file_path
    title = options.title
    institute = options.institute
    description = options.description
    test_run = options.dry_run
    mytardis_url = options.mytardis_url
    username = options.username
    apikey = options.apikey

    mytardis_swift_uploader = MyTardisSwiftUploader(mytardis_url,
                                                    username,
                                                    apikey)

    mytardis_swift_uploader.upload_directory(file_path,
                                             title=title,
                                             description=description,
                                             institute=institute,
                                             test_run=test_run)

if __name__ == "__main__":
    run()
