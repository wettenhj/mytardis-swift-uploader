# MyTardis / RDSI upload script, used for IonTorrent next-gen seq data.
# MyTardis Swift Uploader v1
# Author: James Wettenhall <james.wettenhall@monash.edu>
# Based on MyTardis Uploader v1.1
# by Steve Androulakis <steve.androulakis@monash.edu>
# Thanks Grischa Meyer <grischa.meyer@monash.edu> for initial script

# This version uses the directory field within the Datafile model,
# in some queries' WHERE clauses, so if you don't already have a
# database index for the directory field, you should create one.

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
import urllib
import urllib2
import sys
import openrc_andrewHill
import subprocess
import traceback

mytardis_base_url = "http://115.146.93.73"
mytardis_username = "hilllab"
mytardis_apikey = "0c0e8965199b5648f5dc7f62d76c31f38922bb5d" # Fake API key for public GitHub version.

swift_container_name = "next_gen_seq"

USE_CHECKSUMS_TO_DETERMINE_FILES_ALREADY_UPLOADED = True
CHECK_IN_SWIFT_BEFORE_POSTING_TO_MYTARDIS = True

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

    def _url_fix(self, url):
        """
        http://stackoverflow.com/questions/120951/how-can-i-normalize-a-url-in-python
        """
        return urllib.quote(url, safe="%/:=&?~#+!$,;'@()*[]")

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

        # The following code is specific to IonTorrent next-gen seq data.
        # It determines the experiment name from explog.txt or expMeta.dat

        exp_title = title or os.path.basename(os.path.abspath(file_path))

        ds_desc = exp_title

        explog_path = os.path.join(os.path.abspath(file_path), "explog.txt")
        explogfinal_path = os.path.join(os.path.abspath(file_path), "explog_final.txt")
        expmeta_path = os.path.join(os.path.abspath(file_path), "expMeta.dat")

        if os.path.exists(explog_path):
            with open(explog_path) as f:
                explog_lines = f.readlines()

            for line in explog_lines:
                if line.startswith("Experiment Name: "):
                    exp_title = line.split("Experiment Name: ")[1].strip()

        if os.path.exists(explogfinal_path):
            with open(explogfinal_path) as f:
                explogfinal_lines = f.readlines()

            for line in explogfinal_lines:
                if line.startswith("Experiment Name: "):
                    exp_title = line.split("Experiment Name: ")[1].strip()

        elif os.path.exists(expmeta_path):
            with open(expmeta_path) as f:
                expmeta_lines = f.readlines()

            for line in expmeta_lines:
                if line.startswith("Run Name = "):
                    exp_title = line.split("Run Name = ")[1].strip()

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

                # This could occur for a broken symbolic link:
                if not os.path.exists(sub_file_path):
                    print "WARNING: Skipping " + sub_file_path
                    continue

                subdirectory = os.path.relpath(dirname, file_path)
                if subdirectory == ".":
                    subdirectory = ""

                ds_id = ds_url.rsplit('/')[-2]
                md5sum = None
                if USE_CHECKSUMS_TO_DETERMINE_FILES_ALREADY_UPLOADED:
                    md5sum = self._md5_file_calc(sub_file_path)
                size = os.path.getsize(sub_file_path)

                # if size > (4*10**9):
                    # print "WARNING: Skipping large file: " + sub_file_path
                    # continue

                datafile_exists_in_mytardis = \
                    self.datafile_exists_in_mytardis(filename, subdirectory,
                                                     ds_id, md5sum)

                datafile_exists_in_swift = False
                if CHECK_IN_SWIFT_BEFORE_POSTING_TO_MYTARDIS and \
                        not datafile_exists_in_mytardis:
                    datafile_exists_in_swift = \
                        self.datafile_exists_in_swift(filename, subdirectory,
                                                      ds_id, ds_desc,
                                                      md5sum)

                if not datafile_exists_in_mytardis and \
                        not datafile_exists_in_swift:
                    print "\t\tUploading file '%s' to dataset '%s'..." % \
                        (sub_file_path, ds_desc)

                    if md5sum is None:
                        md5sum = self._md5_file_calc(sub_file_path)

                    ds_id = ds_url.split("/")[-2]

                    # f_url = "/test/"
                    if not test_run:
                        ds_path = self._get_path_from_url(ds_url)
                        location = self.upload_file(sub_file_path,
                                                    subdirectory,
                                                    ds_path,
                                                    ds_desc, ds_id,
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

                            ds_path = self._get_path_from_url(ds_url)
                            location = self.upload_file(sub_file_path,
                                                        subdirectory,
                                                        ds_path,
                                                        ds_desc, ds_id,
                                                        md5sum, size)
                        # print f_url
                elif not datafile_exists_in_mytardis and \
                        datafile_exists_in_swift:
                    ds_id = ds_url.split("/")[-2]

                    replicas = [{
                        "url": os.path.join(ds_desc + "-" + ds_id, subdirectory, filename),
                        "location": "swift",
                        "protocol": "file",
                        "verified": False
                        }]

                    mimetype = mimetypes.guess_type(sub_file_path)[0]
                    if sub_file_path.endswith(".sam") or \
                            sub_file_path.endswith("expMeta.dat") or \
                            sub_file_path.endswith("uploadStatus") or \
                            sub_file_path.endswith(".summary") or \
                            sub_file_path.endswith(".conf") or \
                            sub_file_path.endswith(".json") or \
                            sub_file_path.endswith(".fasta") or \
                            sub_file_path.endswith(".fai") or \
                            sub_file_path.endswith(".php") or \
                            sub_file_path.endswith(".key") or \
                            sub_file_path.endswith(".parsed") or \
                            sub_file_path.endswith(".stats") or \
                            sub_file_path.endswith(".histo.dat") or \
                            sub_file_path.endswith(".log"):
                        mimetype = "text/plain"

                    ds_path = self._get_path_from_url(ds_url)
                    location = self.register_file(ds_path, filename,
                                                  subdirectory, md5sum,
                                                  size, mimetype, replicas)
                    if location:
                        print "\t\tSuccessfully registered %s in MyTardis." % \
                            os.path.join(subdirectory, filename)
                    else:
                        print "\t\tFailed to register %s in MyTardis." % \
                            os.path.join(subdirectory, filename)
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
            print traceback.format_exc()
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
                                    ds_id, md5sum):

        url = self.mytardis_url + "/api/v1/dataset_file/?format=json" + \
            "&dataset__id=" + ds_id + \
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
                                 ds_id, ds_desc, md5sum):
        """
        Skips MD5 check if md5sum is None.
        """
        swift_object_name = self._url_fix(ds_desc + "-" + ds_id + \
            "/" + os.path.join(subdirectory, filename))

        swift_stat_cmd = "swift stat \"%s\" \"%s\"" % (swift_container_name,
                                                       swift_object_name)
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
                if md5sum is None:
                    object_found = True
                else:
                    swift_object_md5sum = \
                        line.strip().split("ETag: ")[1]
                    object_found = (md5sum == swift_object_md5sum)
                    if not object_found:
                        print "Swift MD5 sum doesn't match for " + \
                            os.path.join(subdirectory, filename)
                break
        if object_found:
            print "\t\t%s was found in Swift." % swift_object_name
            return True

        return False

    def upload_file(self, file_path, subdirectory,
                    ds_path, ds_desc, ds_id,
                    md5sum, size):

        filename = os.path.basename(file_path)

        UPLOAD_TO_SWIFT_FIRST_THEN_REGISTER_IN_MYTARDIS = True
        POST_TO_MYTARDIS_VIA_TASTYPIE_API = False

        # The spaces in filenames issue affects using Python SwifClient's
        # command-line interface, but the problem would probably disappear
        # if we just imported swiftclient as a regular Python module.
        if " " in filename:
            UPLOAD_TO_SWIFT_FIRST_THEN_REGISTER_IN_MYTARDIS = False
            POST_TO_MYTARDIS_VIA_TASTYPIE_API = True

        if UPLOAD_TO_SWIFT_FIRST_THEN_REGISTER_IN_MYTARDIS:
            swift_object_name = self._url_fix(ds_desc + "-" + ds_id + \
                "/" + os.path.join(subdirectory, filename))

            one_gigabyte = 1073741824
            if size > one_gigabyte:
                segmentation = "--segment-size %d" % one_gigabyte
            else:
                segmentation = ""

            swift_upload_cmd = \
                "swift upload %s --object-name=\"%s\" \"%s\" \"%s\"" \
                % (segmentation, swift_object_name,
                   swift_container_name, file_path)
            # print "\t\tswift_upload_cmd: " + str(swift_upload_cmd)
            swift_upload_proc = \
                subprocess.Popen(swift_upload_cmd,
                                 stdout=subprocess.PIPE,
                                 stdin=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 shell=True,
                                 universal_newlines=True)
            stdout, stderr = swift_upload_proc.communicate()
            lines = stdout.split("\n")
            success = (swift_upload_proc.returncode == 0)
            if success:
                print "\t\tSuccessfully uploaded %s to Swift." % swift_object_name
            else:
                print "\t\tFailed to upload %s to Swift." % swift_object_name
                print stderr
                print stdout
                return None

            mimetype = mimetypes.guess_type(file_path)[0]
            if file_path.endswith(".sam") or \
                    file_path.endswith("expMeta.dat") or \
                    file_path.endswith("uploadStatus") or \
                    file_path.endswith(".summary") or \
                    file_path.endswith(".conf") or \
                    file_path.endswith(".json") or \
                    file_path.endswith(".fasta") or \
                    file_path.endswith(".fai") or \
                    file_path.endswith(".php") or \
                    file_path.endswith(".histo.dat") or \
                    file_path.endswith(".log"):
                mimetype = "text/plain"

            replicas = [{
                "url": os.path.join(ds_desc + "-" + ds_id, subdirectory, filename),
                "location": "swift",
                "protocol": "file",
                "verified": False
                }]
            location = self.register_file(ds_path, filename, subdirectory,
                                          md5sum, size, mimetype, replicas)
            if location is not None:
                print "\t\tSuccessfully registered %s in MyTardis." % swift_object_name
            else:
                print "\t\tFailed to register %s in MyTardis." % swift_object_name

            return location
        else:
	    mimetype = mimetypes.guess_type(file_path)[0]
            if file_path.endswith(".sam") or \
                    file_path.endswith("expMeta.dat") or \
                    file_path.endswith("uploadStatus") or \
                    file_path.endswith(".summary") or \
                    file_path.endswith(".conf") or \
                    file_path.endswith(".json") or \
                    file_path.endswith(".fasta") or \
                    file_path.endswith(".fai") or \
                    file_path.endswith(".php") or \
                    file_path.endswith(".histo.dat") or \
                    file_path.endswith(".log"):
                mimetype = "text/plain"
            datafile_dict = {u'dataset': ds_path,
                             u'filename': os.path.basename(file_path),
                             u'directory': subdirectory,
                             u'md5sum': md5sum,
                             u'mimetype': mimetype,
                             u'size': size,
                             u'parameter_sets': []}

            datafile_json = json.dumps(datafile_dict)
            data = self._send_datafile(datafile_json, 'dataset_file/', 'POST',
                                       file_path)
            if data is None:
                return None

            return data.headers['location']

    def register_file(self, ds_path, filename, subdirectory,
                      md5sum, size, mimetype, replicas):

        datafile_dict = {u'dataset': ds_path,
                         u'filename': filename,
                         u'directory': subdirectory,
                         u'md5sum': md5sum,
                         u'size': size,
                         u'mimetype': mimetype,
                         u'replicas': replicas,
                         u'parameter_sets': []}

        datafile_json = json.dumps(datafile_dict)

        data = self._send_data(datafile_json, 'dataset_file/')

        if data is None:
            return None

        return data.headers['location']


def run():

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
