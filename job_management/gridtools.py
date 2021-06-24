import hashlib
import os
import re
import sys
from os.path import exists
from subprocess import Popen, PIPE
from zlib import adler32
from time import time
import gfal2
import string
import inspect
import couchdb
import credentials
import datetime

import pickle
import tempfile
import logging
import shutil


from picas.actors import RunActor
from picas.clients import CouchDB
from picas.iterators import TaskViewIterator
from picas.modifiers import BasicTokenModifier



logger = logging.getLogger("mylogger")

#
# try:  # Python 2.7+
#     from logging import NullHandler
# except ImportError:
#     class NullHandler(logging.Handler):
#
#         def emit(self, record):
#             pass
#
# logging.getLogger(__name__).addHandler(NullHandler())
#
# logger = logging.getLogger(__name__)




class CouchDBLogHandler(logging.StreamHandler, object):


    def __init__(self, client, token):

        super(CouchDBLogHandler, self).__init__()
        self.client = client
        self.token=token

    def format(self, record):

        """
        """
        
        return dict(
            message=record.msg,
            level=record.levelname,
            created=int(record.created),
            line=record.lineno
        )

    def emit(self, record):
        """
        """
        if self.token:
            #token = self.client.db[self.tokenid]
            if "log" not in self.token:
                self.token["log"] = [self.format(record)]
            else:
                self.token["log"].append(self.format(record))
            
            self.token["_rev"]=self.client.db.save(self.token.value)[1]

        else:
            self.client.db.save(self.format(record))




class MineRunner(RunActor):

    """
    """

    def __init__(self, iterator, modifier):
        """
        """
        # RunActor.__init__(iterator, modifier)
        # This is what happens in RunActor:
        self.iterator = iterator
        self.modifier = modifier
        self.token = None
        self.token_rev = None
        # Make a copy of the db reference
        self.client=iterator.database
        self.db = iterator.database
        self.tasks_processed=0
        

    def download_all_files(self, files_remote_map):
        """
        """
        filemap = {}
        for filetype in files_remote_map:
            remotefile = str(files_remote_map[filetype]["url"])
            basename_remote = os.path.basename(remotefile)
            adler_hash = None
            md5_hash = None
            if "adler32" in files_remote_map[filetype]:
                adler_hash = str(files_remote_map[filetype]["adler32"])
            if "md5" in files_remote_map[filetype]:
                md5_hash = str(files_remote_map[filetype]["md5"])
            localfile = os.getcwd() + "/" + basename_remote
            download_file(
                remotefile,
                localfile,
                adler32Hash=adler_hash,
                md5Hash=md5_hash)
            filemap[filetype] = localfile

        return filemap


    def execute(self, args, shell=False, logname_extention="", stdout_fh=PIPE):
        """Helper function to more easily execute applications.
        @param args: the arguments as they need to be specified for Popen.
       @return: a dictonary containing the command,duration,exitcode, stdout & stderr
        """

        if isinstance(args, list):
            logger.debug(" ".join(args))
        else:
           logger.debug(args)
        #convert unicode strings to normal strings 
        args=[str(s) for s in args]
        start = time()
        logger.debug(args)
        try:
            proc = Popen(args, stdout=stdout_fh, stderr=PIPE, shell=shell)
        except OSError as e:

            logger.error(str(e))
           
            logger.error(args)
            exit(1)
        except:
            e = sys.exc_info()[0]
            print (e)
            logger.error("Failed execute")
            logger.error(args)
            exit(1)


        (stdout, stderr) = proc.communicate()
        end = time()
        # round duration to two floating points
        duration = int((end - start) * 100) / 100.0

        if proc.returncode != 0:
            logger.error("exit code not 0: " + str(proc.returncode))
            logger.error(" ".join(args))
            logger.error("stdout:" + stdout)
            logger.error("stderr:" + stderr)

        job_report = {"command": " ".join(args),
                      "exitcode": proc.returncode,
                      "stdout": stdout,
                      "stderr": stderr,
                      "duration": duration}

        f = inspect.currentframe().f_back
        functname = f.f_code.co_name
        lineno = f.f_lineno
        atachment_name = str(functname) + "_" + str(lineno) + logname_extention

        self.client.db.put_attachment(self.token.value, str(job_report), atachment_name)
        self.token.update(self.client.db[self.token["_id"]])

        if proc.returncode != 0:
            exit(1)
        return job_report
        
    def worker(self, token):
        pass

    def prepare_env(self, *kargs, **kvargs):
        pass

    def prepare_run(self, *kargs, **kvargs):
        pass

    def process_task(self, token):
        # this is where all the work gets done. Start editing here.
        self.token = token


        # add new handler which logs to token
        logger.addHandler(CouchDBLogHandler(
            self.client,
            token))
        

        if 'TMPDIR' not in globals():
            TMPDIR = os.environ['TMPDIR'] + "/"


        #create a new working enviroment
        working_dir = TMPDIR + str(token["_id"])
        os.mkdir(working_dir)
        os.chdir(working_dir)

        self.worker(token)

        # remove all local files
        os.chdir(TMPDIR)
        shutil.rmtree(working_dir)


        token = self.modifier.close(token)


    def cleanup_run(self, *kargs, **kvargs):
        pass

    def cleanup_env(self, *kargs, **kvargs):
        pass



def execute(args, shell=False, ignore_returncode=False):
    """Helper function to more easily execute applications.
    @param args: the arguments as they need to be specified for Popen.
    @return: a tuple containing the exitcode, stdout & stderr
    """

    if isinstance(args, list):
        logger.debug(" ".join(args))
    else:
        logger.debug(args)

    try:
        proc = Popen(args, stdout=PIPE, stderr=PIPE, shell=shell)
    except OSError as e:
        logger.debug(e)
        logger.debug(args)
        exit(1)
    except:
        e = sys.exc_info()[0]
        print(e)
        logger.error("Failed execute")
        logger.error(args)
        exit(1)
    (stdout, stderr) = proc.communicate()
    if not ignore_returncode:
        if proc.returncode != 0:
            logger.error("exit code not 0: " + str(proc.returncode))
            logger.error(" ".join(args))
            logger.error("stdout:" + stdout)
            logger.error("stderr:" + stderr)

            sys.exit(1)
    job_report = {"exitcode": proc.returncode,
                  "stdout": stdout,
                  "stderr": stderr}

    return job_report


def templater(raw_template):
    """
    merge the locals of prevoius function into the dict allvars.
    then fill the variablbles with the template enginge
    """
    logger = logging.getLogger(__name__)


    frame = inspect.currentframe()
    old_locals = frame.f_back.f_locals
    logger.debug(str(old_locals))
    # allvars = path_dict.copy()
    allvars = {}
    allvars.update(old_locals)
    logger.debug(str(old_locals))
    logger.debug(str(raw_template))
    raw_template = string.Template(raw_template)
    raw_template.substitute(allvars)
    return (raw_template.substitute(old_locals))


def run_as_bash(commands):

    lastfunction = inspect.currentframe().f_back
    functname = lastfunction.f_code.co_name
    lineno = lastfunction.f_lineno
    filename = str(functname) + "_" + str(lineno) + ".sh"

    fh = open(filename, 'w')

    bashheader = """#!/bin/bash
    set -x
    set -u
    set -e
    set -o pipefail

    """

    fh.write(bashheader)
    fh.write(commands)
    # delete the script after running
    fh.write("\n rm " + filename)

    fh.close()
    logger.debug(fh.closed)

    return(["/bin/bash", os.getcwd()+"/"+filename])


def md5_of_file(filepath):
    """
    Calculate md5 checksum of filepath
    """
    afile = open(filepath, 'rb')
    hasher = hashlib.md5()
    blocksize = 65536
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()


def adler32_of_file(filepath):
    """
        Calculate adler32 checksum of filepath

    """
    BLOCKSIZE = 1048576  # that's 1 MB

    asum = 1
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(BLOCKSIZE)
            if not data:
                break
            asum = adler32(data, asum)
            if asum < 0:
                asum += 2 ** 32

    return hex(asum)[2:10].zfill(8).lower()


def connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME,
                       password=credentials.PASS, dbname=credentials.DBNAME):
    """
    returns a object couchdb (not truly connect since couchdb is RESTFULL and no separate conection is made for autorization and authendication)
    """
    db = couchdb.Database(url + "/" + dbname)
    db.resource.credentials = (username, password)
    return db


def get_adler32_on_srm(remote_surl):
    """
    retrive adler32 hash via the srmls tools
    """
#     cmd = ["srmls", "-l", remote_surl]
#     job_report = execute(cmd)  # extract adler32 of srm with adler32 of token
#     all_adler = re.findall(
#         "Checksum value:\s*([0-9a-z]*)", job_report["stdout"])
#     if len(all_adler) == 1:
#         adler_srm = all_adler[0]
#     else:
#         logger.error(
#             "could not extract right amount of hashes:" + str(all_adler) + str(all_adler))
#         exit(1)
    ctx = gfal2.creat_context()
    logger.debug(remote_surl)
    adler_srm = ctx.checksum(str(remote_surl), "adler32")

    return adler_srm


def convert_to_surl(url):
    """
    Convert a turl/surl to a tupple of (surl,turl)
    """
    surl = None
    turl = None
    if str(url).startswith("srm://"):
        surl = url
        turl = url.replace(
            "srm://srm.grid.sara.nl", "gsiftp://gridftp.grid.sara.nl", 1)
    elif str(url).startswith("gsiftp://"):
        turl = url
        surl = re.sub(
            r"gsiftp://gridftp.grid.sara.nl/",
            "srm://srm.grid.sara.nl/",
            url)
        # url.replace("gsiftp://gridftp.grid.sara.nl:2811",
        # "srm://srm.grid.sara.nl", 1)
    else:
        logger.warning(
            "Downloading failed: url did not start with gsiftp:// or srm:// :" +
            str(url))
    return surl, turl




def is_local_file(url):
    """
    Return true if url is existing local file and false if the file surl/turl

    exit if file does not exists localy
    """
    local_file = False
    if url.startswith("srm://"):
        local_file = False
    elif url.startswith("gsiftp://"):
        local_file = False
    elif os.path.isfile(url):
        local_file = True
    else:
        logger.error(
            str(url) + "could not be identified as SURL TURL or localfile")
        exit(1)

    return local_file

def download_all_files(files_remote_map):
    """
    """
    filemap = {}
    for filetype in files_remote_map:
        remotefile = str(files_remote_map[filetype]["url"])
        basename_remote = os.path.basename(remotefile)
        adler_hash = None
        md5_hash = None
        if "adler32" in files_remote_map[filetype]:
            adler_hash = str(files_remote_map[filetype]["adler32"])
        if "md5" in files_remote_map[filetype]:
            md5_hash = str(files_remote_map[filetype]["md5"])
        localfile = os.getcwd() + "/" + basename_remote
        download_file(
            remotefile,
            localfile,
            adler32Hash=adler_hash,
            md5Hash=md5_hash)
        filemap[filetype] = localfile

    return filemap


def download_file(url, localfile, adler32Hash=None, md5Hash=None):
    """
    Download a file
    @url a srm, turl or local path
        - srm prefix is srm://
        - turl prefix is gsiftp://
        - local path has no prefix
    @localfile destignation of file
    @adler32Hash adler32 hash to verify the copied file
    @md5Hash md5 hash to verify the copied file
    """
    # verify location with srm
    # logger=logging.getLogger(__name__)

    local_copy = is_local_file(url)

    surl, turl = convert_to_surl(url)
    # srmcopy

    if not local_copy:
        starttime = time()
        adler_srm = get_adler32_on_srm(surl)
        if adler32Hash is not None:
            if adler_srm != adler32Hash:
                logger.exception(
                    "Provided adler32 not the same as on SRM adler32 on server:" +
                    adler_srm +
                    " adler32 on server:" +
                    str(adler32Hash))

        srmtime = time()
        # download file with uberftp
        uberftp_localfile = localfile
        if not uberftp_localfile.startswith("file://"):
            uberftp_localfile = "file://" + localfile
        cmd = ["uberftp", "-parallel", "4",
               "-retry", "3", turl, uberftp_localfile]
        job_report = execute(cmd)
        copytime = time()
        logger.debug(job_report)
    else:
        # copy on local files system
        cmd = ["cp", url, localfile]
        job_report = execute(cmd)
    # verify with adler 32 if file is the same
    if adler32Hash is not None:
        adler32_local = adler32_of_file(localfile)
        adlertime = time()
        srmtimenetto = str(srmtime - starttime)
        copytimenetto = str(copytime - srmtime)
        adlertimenetto = str(adlertime - copytime)
        logger.debug("srmtimenetto:" + srmtimenetto + "  copytimenetto:" +
                     copytimenetto + " adlertimenetto:" + adlertimenetto)
        if adler32_local != adler32Hash:
            logger.exception(
                "Incorrect adler32 hash for localfile" +
                localfile +
                "(" +
                adler32_local +
                ")" +
                "downloaded from" +
                url +
                "(" +
                adler32Hash +
                ")")
            exit(1)
    # verify with adler 32 if file is the same
    if md5Hash is not None:
        md5_local = md5_of_file(localfile)
        if md5_local != md5Hash:
            logger.exception(
                "Incorrect md5 hash for localfile" +
                localfile +
                "(" +
                md5_local +
                ")" +
                "downloaded from" +
                url +
                "(" +
                md5Hash +
                ")")
            exit(1)


def rm_srm_flle(remote_surl):
    """
    removes surl with srm command line tools
    """
    cmd = ["srmrm", remote_surl]
    jobreport = execute(cmd)
    return jobreport


def rm_file(url):
    """
    remove a file independed on of surl/turl or local file (using local rm of uberftp for gridftp)
    """
    local_copy = is_local_file(url)

    surl, turl = convert_to_surl(url)

    if not local_copy:
        cmd = ["uberftp", "-rm", turl]
        job_report = execute(cmd)
    else:
        # copy on local files system
        cmd = ["rm", "-f", url]
        job_report = execute(cmd)


def rm_dir(url):
    """
    remove a file depending on url which method(using local rm of uberftp for gridftp)
    """
    local_copy = is_local_file(url)

    surl, turl = convert_to_surl(url)

    if not local_copy:
        cmd = ["uberftp", "-rmdir", turl]
        job_report = execute(cmd)
    else:
        # copy on local files system
        cmd = ["rmdir", url]
        job_report = execute(cmd)


def upload_file(local_file, remote_surl):
    """
    uploads a file to grid storage or localdisk
    when upload to gridftp a adler32 hash is calculated localy and compared with the adler32 in srm system
    """

    logger = logging.getLogger(__name__)

    # check localfile exisits
    if exists(local_file) is False:
        print("does not exisit during")
        logging.error(
            local_file + "does not exist during upload to" + remote_surl)
        logger.error(
            local_file + "does not exist during upload to" + remote_surl)
        exit()
    logger.debug("etste")

    # get loal adler32
    adler32_local = adler32_of_file(local_file)

    if not is_local_file(remote_surl):
        # format the surl alright
        surl, turl = convert_to_surl(remote_surl)

        cmd = ["globus-url-copy", "-create-dest",
               "-rst", "-p", "4", local_file, turl]
        print("pre command")
        job_report = execute(cmd)
        print("logger.debug(job_report)")
        logger.debug(job_report)
        print("post logger.debug(job_report)")
        # get remote adler32
        adler_srm = get_adler32_on_srm(surl)
        # check id they are sam
        if adler32_local != adler_srm:
            logger.error(
                "remote and local adler32 are unequel after upload of localfile" +
                local_file +
                "and remote file" +
                remote_surl)
            job_report = rm_srm_flle(surl)
            exit()
    else:
        # logger.debug("using local copy from "+ local_file+" to "+ remote_surl)
        # logger.debug("creating dir: "+os.path.dirname(remote_surl))
        cmd = ["mkdir", "-p", os.path.dirname(remote_surl)]
        job_report = execute(cmd)
        cmd = ["cp", local_file, remote_surl]
        job_report = execute(cmd)

    return adler32_local


def wait2stage_to_stage(n=100):
    """
    Converts wait2stage tokens to to_stage state, so in next iteration of the stage script the files can be staged
    """
    logger = logging.getLogger(__name__)


    db = connect_to_couchdb(
        credentials.URL,
        credentials.USERNAME,
        credentials.PASS,
        credentials.DBNAME)
    to_update = []

    i = 0
    for row in db.iterview(credentials.VIEW_NAME + "/wait_to_stage", 100):
        if (i < n):
            doc = row["value"]
            doc["stage_lock"] = 0
            # delete all attachments if present
            to_update.append(doc)
            i = i + 1
        else:
            break

    db.update(to_update)
    logging.info("converted " + str(i) + " tokens from wait2stage to stage")
    #print("Do not forget to run the staging script!!")


def reset_staging(hour=24):
    '''

    reset the tokens older the hour hours
    '''
    db = connect_to_couchdb(
        credentials.URL,
        credentials.USERNAME,
        credentials.PASS,
        credentials.DBNAME)
    max_age = time() - (hour * 3600)
    to_update = []
    for row in db.iterview(credentials.VIEW_NAME + "/staging", 100):
        doc = row["value"]
        if (doc["stage_lock"] < max_age):
            doc["stage_lock"] = -1
            to_update.append(doc)

    db.update(to_update)


def create_views():
    """
    creates the views in couchdb on basis of credentials in credentials.py
    this script should only run ones
    """

    view_js = {
        "language": "javascript",
        "views": {
            "overview": {
                "map": "function (doc) { if (doc.type===\"TYPE\"){if (doc.stage_lock<0 && doc.stage_done==0 && doc.lock==0 && doc.done==0) {emit(\"1wait_to_stage\", 1);}  if (doc.stage_lock==0 && doc.stage_done==0 && doc.lock==0 && doc.done==0) {emit(\"2to_stage\", 1);}  if (doc.stage_lock>0 && doc.stage_done==0 && doc.lock==0 && doc.done==0) {emit(\"3staging\", 1);}  if (doc.stage_lock>0 && doc.stage_done>0 && doc.lock==0 && doc.done==0) {emit(\"4todo\", 1);}  if(doc.lock>0 && doc.done==0) {emit(\"5locked\", 1);}   if ( doc.lock==-1 && doc.done==-1) {emit(\"6error\", 1);}  if ( doc.lock>0 && doc.done>0) {emit(\"7done\", 1);}       }       }",
                "reduce": "_sum"
            },
            "to_stage": {
                "map": "function (doc) { if (doc.type==\"TYPE\"){if (doc.stage_lock==0 && doc.stage_done==0 && doc.lock==0 && doc.done==0) {emit(doc._id, doc);}}}"
            },
            "todo": {
                "map": "function (doc) { if (doc.type==\"TYPE\"){if (doc.stage_lock>0 && doc.stage_done>0 && doc.lock==0 && doc.done==0) {emit(doc._id, doc._id);}}}"
            },
            "locked": {
                "map": "function (doc) { if (doc.type==\"TYPE\"){if (doc.lock>0 && doc.done==0) {emit(doc._id, doc);}}}"
            },
            "done": {
                "map": "function (doc) {if (doc.type==\"TYPE\"){if ( doc.lock>0 && doc.done>0) {emit(doc._id, doc);}}}"
            },
            "staging": {
                "map": "function (doc) { if (doc.type==\"TYPE\"){if (doc.stage_lock>0 && doc.stage_done==0 && doc.lock==0 && doc.done==0) {emit(doc._id, doc);}}}"
            },
            "wait_to_stage": {
                "map": "function (doc) { if (doc.type==\"TYPE\"){if (doc.stage_lock<0 && doc.stage_done==0 && doc.lock==0 && doc.done==0) {emit(doc._id, doc);}}}"
            },
            "error": {
                "map": "function (doc) { if (doc.type==\"TYPE\"){if ( doc.lock==-1 && doc.done==-1) {emit(doc._id, doc);}}}"
            }
        }
    }

    for key in view_js["views"].keys():
        view_js["views"][key]["map"] = view_js["views"][key][
            "map"].replace("TYPE", credentials.VIEW_NAME)

    print("modified view" + str(view_js))
    db = connect_to_couchdb(
        credentials.URL,
        credentials.USERNAME,
        credentials.PASS,
        credentials.DBNAME)
    print("connected to db")

    db["_design/" + credentials.VIEW_NAME] = view_js
    print("uploaded to DB")
    print("Done")


def overview_token_states():
    db = connect_to_couchdb()
    overview_raw = db.view(credentials.VIEW_NAME + "/overview", group_level=1)

    overview = {}
    for a in overview_raw.rows:
        overview[a.key] = int(a.value)

    return(overview)

def totaljobs_to_proccess():
    token_states=overview_token_states()
    waiting_states=["4todo","1wait2stage","2tostage","3staging","todo","wait2stage","staging","to_stage"]
    total_jobs_waiting=0
    for state in waiting_states:
        if state in token_states:
            total_jobs_waiting=+token_states[state]

    return total_jobs_waiting

def docs_in_view(view, viewname=credentials.VIEW_NAME):
    """
    return amount of docs in a view
    """
    db = connect_to_couchdb()
    return (db.view(viewname + "/" + view).total_rows)


def n_files_online(surls):
    """
    Get the number of files back that are staged
    """
    logger = logging.getLogger(__name__)

    surls=[convert_to_surl(turl)[0] for turl in surls]

    ctx = gfal2.creat_context()
    files_online = 0
    for surl in surls:
        try:
            status = ctx.getxattr(surl, "user.status")
            if (status == "ONLINE_AND_NEARLINE"):
                files_online = files_online + 1
        except gfal2.GError as e:
            if 'No such file' in e.message:
                logger.error(str(e))
                status = 1
    return(files_online)


def check_staged_tokens():
    """
    check of tokens that are in todo stage are truely staged.
    If not staged they are transformed to "to_stage"

    """

    logger = logging.getLogger(__name__)
    jobs_availble = docs_in_view("todo")

    if(jobs_availble == 0):
        print("no jobs availble in todo state")
    else:
        db = connect_to_couchdb()

        for row in db.iterview(credentials.VIEW_NAME + "/todo", 100):
            doc = db[row["id"]]

            logger.debug(doc)
            files_remote_map = doc["files"]
            turls = []
            for filetype in files_remote_map:
                turls.append(str(files_remote_map[filetype]["url"]))
            m = re.compile('.*/pnfs')
            surls = [
                m.sub(
                    'srm://srm.grid.sara.nl:8443/srm/managerv2?SFN=/pnfs',
                    k) for k in turls]
            if(n_files_online(surls) == len(surls)):
                print("all " + str(len(surls)) +
                      " files online of token:" + str(doc["_id"]))
            else:
                print("not all files(" +
                      str(n_files_online) +
                      "/" +
                      str(len(surls)) +
                      ") online:" +
                      str(doc["_id"]) +
                      " reseting token")
                doc["stage_lock"] = 0
                doc["stage_done"] = 0
                doc["lock"] = 0
                doc["done"] = 0

                db.update([doc])


def pin_files(surls, pintime=10800):
    """
    use a list of surls to pin for a period (by default3 hours).
    this prevents files being purged from disk while waiting to be dowloaded
    """
    for surl in surls:
        ctx = gfal2.creat_context()
        try:
            # bring_online(surl, pintime, timeout, async)
            (status, token) = ctx.bring_online(surl, pintime, pintime, False)
            while status == 0:
                status = ctx.bring_online_poll(surl, token)
        except gfal2.GError as e:
            print("Could not bring the file online:")
            print("\t", e.message)
            print("\t Code", e.code)


def check_unstaged_tokens():
    """
    Check tokens in the "wait_to_stage", "to_stage", "staging"  state are already staged.
    If they are staged tokens will tranformed to todo state
    """

    logger = logging.getLogger(__name__)

    tokenstates = overview_token_states()
    logging.debug(str(tokenstates))
    db = connect_to_couchdb()
    for view in ["wait_to_stage", "to_stage", "staging"]:
        for row in db.iterview(credentials.VIEW_NAME + "/" + str(view), 100):
           doc = db[row["id"]]
           if check_token_online(doc):
                if(doc["stage_done"] <= 0):
                    doc["stage_done"] = int(time())
                if(doc["stage_lock"] <= 0):
                    doc["stage_lock"] = int(time())
                db.update([doc])


    tokenstates = overview_token_states()
    logger.debug(tokenstates)
    return(tokenstates)

def check_token_online(doc):

            logger.debug(doc)
            files_remote_map = doc["files"]
            turls = []
            for filetype in files_remote_map:
                turls.append(str(files_remote_map[filetype]["url"]))
            m = re.compile('.*/pnfs')
            surls = [
                m.sub(
                    'srm://srm.grid.sara.nl:8443/srm/managerv2?SFN=/pnfs',
                    k) for k in turls]
            if(n_files_online(surls) == len(surls)):

                # pin the files for e few hours to prevent them to be purged
                # from disk
                pin_files(surls)
                logger.debug("all " + str(len(surls)) +
                      " files online of token: " + str(doc["_id"]))
            
                return True
            else:
                logger.debug(str(doc["_id"]) + "not online")
                return False

def autosubmit(filename, max_job_size):
    """
    submits jobs if there are tokens in todo state.
    Number of jobs send is equal to tokens in "todo" state with a maximum of max_job_size

    """
    logger = logging.getLogger(__name__)

    job_availble = docs_in_view("todo")

    if(job_availble == 0):
        logger.info("\tno jobs availble")
    else:
        amount_to_submit = min(job_availble, max_job_size)
        pwd = os.getcwd()
        temp_jdl = tempfile.NamedTemporaryFile(
            suffix=".jdl", prefix="tmp", dir=pwd, delete=False)
        pattern_compiled = re.compile("Parameters\w?=\w?(\d.)")

        with open(filename) as src_file:
            for line in src_file:
                temp_jdl.write(
                    pattern_compiled.sub(
                        "Parameters=" +
                        str(amount_to_submit) +
                        ";",
                        line))

        temp_jdl.close()
        temp_jdl_path = os.path.basename(temp_jdl.name)
        user = os.environ["USER"]
        cmd = ["glite-wms-job-submit", "-d",
               user, "-o", "myjobs", temp_jdl_path]
        res = execute(cmd)
        if (int(res["exitcode"]) == 0):
            logger.info("submitted " + str(amount_to_submit) + " jobs")

        else:
            logger.error(str(res))

        # proc = Popen(args, stdout=PIPE, stderr=PIPE, shell=False)
        os.remove(temp_jdl.name)


def check_jobs_in_queue(ce_adress):

    cmd = "glite-ce-job-status --all -e  " + \
        ce_adress + " -s IDLE  |grep Status |wc -l"
    res = execute(cmd, shell="/bin/bash")
    return int(res["stdout"].strip())


def multicoreautosubmit(filename, max_job_size):
    """
    submits jobs if there are tokens in todo state.
    Number of jobs send is equal to tokens in "todo" state with a maximum of max_job_size

    """
    logger = logging.getLogger(__name__)

    job_availble = docs_in_view("todo", "all")
    job_availble_mc = docs_in_view("todo", "SamToFastqAndBwaMem")
    logger.debug("multi core: " + str(job_availble_mc) +
                 "\t\tsingle core: " + str(job_availble))
    if(job_availble == 0):
        logger.info("\tno jobs availble")
    else:
        if job_availble_mc != 0:
            queued = check_jobs_in_queue("creamce.gina.sara.nl:8443") - 1
            logger.debug("queed jobs" + str(queued))
            mcjobstosubmit = max(0, job_availble_mc - queued)

            # set a maximum off amount of jobs to submit
            mcjobstosubmit = min(int(max_job_size) / 2, mcjobstosubmit)
            logger.debug("submiting multicore jobs:" + str(mcjobstosubmit))
            logger.debug(os.getcwd())
            for n in range(mcjobstosubmit):
                cmd = [
                    "glite-ce-job-submit",
                    "-d",
                    "-a",
                    "-r",
                    "creamce.gina.sara.nl:8443/cream-pbs-mediummc",
                    "recalibrate8cores.jdl"]
                res = execute(cmd)
                logger.debug(str(res))
            logger.debug("submiting multicore jobs:" + str(mcjobstosubmit))

        amount_to_submit = min(job_availble - job_availble_mc, max_job_size)

        if amount_to_submit > 0:
            pwd = os.getcwd()
            temp_jdl = tempfile.NamedTemporaryFile(
                suffix=".jdl", prefix="tmp", dir=pwd, delete=False)
            pattern_compiled = re.compile("Parameters\w?=\w?(\d.)")

            with open(filename) as src_file:
                for line in src_file:
                    temp_jdl.write(
                        pattern_compiled.sub(
                            "Parameters=" +
                            str(amount_to_submit) +
                            ";",
                            line))

            temp_jdl.close()
            temp_jdl_path = os.path.basename(temp_jdl.name)
            user = os.environ["USER"]
            cmd = ["glite-wms-job-submit", "-d",
                   user, "-o", "myjobs", temp_jdl_path]
            res = execute(cmd)
            if (int(res["exitcode"]) == 0):
                logger.info("submitted " + str(amount_to_submit) + " jobs")

            else:
                logger.error(str(res))

            # proc = Popen(args, stdout=PIPE, stderr=PIPE, shell=False)
            os.remove(temp_jdl.name)


def retrieve_tokens_from_tokendb(country="all_countries"):
    """

    """

    # todo: create a nice dinamic way to create this set
    availble_countries = set(["Belgium",
                              "Ireland",
                              "Netherlands",
                              "NL",
                              "Spain",
                              "test",
                              "Turkey",
                              "UK",
                              "US"])

    db = connect_to_couchdb(dbname="project_mine_tokens")

    alldocs = []

    if (country == "all_countries"):
        dbiterator = db.iterview("country/country", 200)
    else:
        if country in availble_countries:
            dbiterator = db.iterview("country/country", 200, key=country)
        else:
            print("wrong country: selected out of:" + str(availble_countries))
            exit(1)

    for doc in dbiterator:
        alldocs.append(doc["value"])

    today = datetime.date.today()
    picklefile = country + "_" + str(today) + ".p"
    pickle.dump(alldocs, open(picklefile, "wb"))
    print("saved tokens to" + picklefile +
          " continue with 'upload_all_tokens('" + picklefile + "')'")
    return(picklefile)


def upload_tokens(pickled_file, limit=None):
    """
    upload the tokens retrieved from the tokens database and saved as
    pickled format. Upload it to the userview and create a unique name based on the original name
    in the token database combined with the viewname.

    """

    orignal_tokens = pickle.load(open(pickled_file, "rb"))
    save_docs_in_db(orignal_tokens[:limit])


def upload_tokens_hg38(pickled_file, limit=None):
    """
    upload the tokens retrieved from the tokens database  to the database speialy made for the HG38
    recalibration pipeline


    """

    orignal_tokens = pickle.load(open(pickled_file, "rb"))
    save_docs_in_db(orignal_tokens[:limit],
                    viewname="SplitInReadGroups", postfix="")


def upload_selected_tokens(pickled_file, idfile):
    """
    upload the tokens retrieved from the tokens database and saved as
    pickled format. Upload it to the userview and create a unique name based on the original name
    in the token database combined with the viewname.

    Only the tokens with in the text file with newline separate id's are uploaded.

    there is also a type field added where the views in couchdb filter on
    """
    orignal_tokens = pickle.load(open(pickled_file, "rb"))
    selected_ids = set([line.strip() for line in open(idfile, 'r')])
    selected_tokens = []

    for token in orignal_tokens:

        if (token["_id"] in selected_ids):
            selected_tokens.append(token)

    print("found " +
          str(len(selected_tokens)) +
          " of " +
          str(len(selected_ids)) +
          " given ids in a heap of" +
          str(len(orignal_tokens)) +
          "tokens")
    save_docs_in_db(selected_tokens)


def save_docs_in_db(docs, viewname=credentials.VIEW_NAME,
                    postfix=credentials.VIEW_NAME):
    """
    Saves documents to CouchDB defined in credentials.py
    If document is present a message will be print and old document is kept unchanged
    """

    db = connect_to_couchdb()
    for token in docs:
        token["sampleid"] = token["_id"]
        token["_id"] = token["_id"] + str(postfix)

        token["type"] = viewname
        if "_rev" in token:
            del token["_rev"]
        try:
            db.save(token)
        except couchdb.ResourceConflict:
            print(token["_id"] + "already in database")


def purge_sample(sampleid="unset"):
    db = connect_to_couchdb()
    ddocs = []
    for row in db.iterview("all/sampleid", 200, key=sampleid):
        # print(row["key"])
        ddocs.append({'_id': row['id'], '_rev': row['value']['_rev']})

    db.purge(ddocs)


def reset_doc_values(doc):
    """
    Reset a document of locked token to prestine "wait_to_stage" state and adds the scrub_count to account amount of fails
    This function returns the focument but saves not to CouchDB
    """
    if "scrub_count" in doc:
        doc["scrub_count"] += 1
    else:
        doc["scrub_count"] = 1
    doc['lock'] = 0
    doc['hostname'] = ''
    if "log" in doc:
        del doc["log"]
    if "error" in doc:
        del doc["error"]

    doc["done"] = 0
# delete all attachments if present
    if "_attachments" in doc:
        del doc["_attachments"]
    if "wms_job_id" in doc:
        del doc["wms_job_id"]
    
    if "stage_done" in doc:
        filesonline=check_token_online(doc)
        if filesonline:
            doc["stage_done"] = int(time())
            doc["stage_lock"] = int(time())
        else:                
            doc["stage_done"] = 0
            doc["stage_lock"] = 0


        

def reset_all_locked_tokens():
    """
    Reset all locked tokens in view to prestine "wait_to_stage" state and adds the scrub_count to account amount of fails
    """
    db = connect_to_couchdb()
    to_update = []
    for row in db.iterview(credentials.VIEW_NAME + "/locked", 100):
        doc = row["value"]
        reset_doc_values(doc)
        to_update.append(doc)

    db.update(to_update)
    print("reseted " + str(len(to_update)) + " tokens")


def reset_locked_tokens(hour=48):
    '''

    Reset  locked tokens  older then hour(defauls =48) in view to prestine "wait_to_stage" state and adds the scrub_count to account amount of fails
    '''
    db = connect_to_couchdb()

    max_age = time() - (hour * 3600)
    to_update = []
    for row in db.iterview(credentials.VIEW_NAME + "/locked", 100):
        doc = row["value"]
        if (doc["lock"] < max_age):
            reset_doc_values(doc)
            to_update.append(doc)

    db.update(to_update)


def purge_tokens_and_view(viewname):
    """
    Delete and purge tokens from view and the view itself
    """
    db = connect_to_couchdb()

    map_fun = "function(doc) {\
    if (doc.type == '"+viewname+"')\
    emit(doc._id, doc._rev);\
    }"

    print("Please wait untill all docs are found in the view {}".format(viewname))
    ddocs=[]
    for row in db.query(map_fun):
        ddocs.append({'_id': row['id'], '_rev': row['value']})
    db.purge(ddocs)
    print("removed {} documents from view {}".format(len(ddocs),viewname))

    #remove view
    view=db["_design/"+viewname]
    ddocs=[{'_id': view.id, '_rev': view.rev}]
    db.purge(ddocs)
    print("removed view {}".format(viewname))


def reset_all_locked_tokens_dev():
    """
    Reset all locked tokens in view to prestine "wait_to_stage" state and adds the scrub_count to account amount of fails
    """
    db = connect_to_couchdb()
    to_update = []
    for row in db.iterview("all/locked", 100):
        doc = row["value"]
        reset_doc_values(doc)
        to_update.append(doc)

    db.update(to_update)
    print("reseted " + str(len(to_update)) + " tokens")


def rm_empty_dir(dirpath):
    surl, turl = convert_to_surl(dirpath)
    if is_local_file(dirpath):
        try:
            os.removedirs(dirpath)
        except OSError as e:
            logger.debug("remove directory failed:" + str(e))

    else:
        #cmd="uberftp -rmdir "+turl
        # execute(cmd,ignore_returncode=True)
        try:
            context = gfal2.creat_context()
            context.rmdir(turl)

        except gfal2.GError as e:
            logger.debug(str(e))
