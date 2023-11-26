<<<<<<< Updated upstream
from os.path import join, relpath, sep, dirname, splitdrive, split
from os import walk, makedirs
=======
from os.path import join, relpath, sep, dirname, splitdrive, split, unlink
from os import walk, makedirs, stat, sep
>>>>>>> Stashed changes
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import threading
import sys
import re

import boto3
import shutil 

class S3ShutilEngine:

    def __init__(self, localroot, s3root):
        self.localroot = localroot
        self.s3root = s3root
        self.thread_local = threading.local()
        self.logger = logging.getLogger('s3shutil')

    def parse_s3_path(self, s3path):
        """"returns a (bucket, fullpath) tuple from a s3://bucket/path/to/key string"""
        assert s3path[:5] == 's3://'
        parts = s3path.split('/')
        return parts[2], '/'.join(parts[3:])

    def enumerate_local(self, root):
        for directory, directories, files in walk(root):
            for f in files:
                full_path = join(directory, f)
                yield full_path

    def get_thread_local(self, attribute, factory_function, *factory_args):
        if hasattr(self.thread_local, attribute):
            return getattr(self.thread_local, attribute)
        else:
            init_value = factory_function(*factory_args)
            setattr(self.thread_local, attribute, init_value)
            return init_value

    def get_local_s3_client(self):
        session = self.get_thread_local('boto3session', boto3.Session)
        s3client = self.get_thread_local('s3client', session.client, 's3')
        return s3client

    def upload(self, localfile):
        relative = relpath(localfile, self.localroot)
        remote_relative = relative.replace(sep, '/')
        s3_path = '%s%s' % (self.s3root, remote_relative)
        self.logger.info('Uploading %s to %s', localfile, s3_path)
        s3client = self.get_local_s3_client()
        bucket, key = self.parse_s3_path(s3_path)
        s3client.upload_file(localfile, bucket, key)

_thread_local_boto3_singleton = None

def get_thread_local_boto3():
    global _thread_local_boto3_singleton
    if _thread_local_boto3_singleton is None:
        _thread_local_boto3_singleton = ThreadLocalBoto3()

    return _thread_local_boto3_singleton


class GenericPath:

    def enumerate_keys(self, root):
        pass

    def delete(self, key):
        pass

    def relative(self, path, start):
        pass

    def join(self, root, relative):
        pass

    def delete_batch_size(self):
        return 1

class FsPath(GenericPath):
    """valid objects are strings"""

    def enumerate_keys(self, root):
        for directory, dirs, files in walk(root):
            if True:
                dirs.sort()
                files.sort()
            for file in files:
                fp = join(directory, file)
                if False:
                    s = stat(fp)
                    last_modified = s.st_mtime
                    sz = s.st_size
                    yield (fp, last_modified, sz)
                else:
                    yield fp

    def delete(self, keys):
        for key in keys:
            unlink(key)

    def relative(self, path, start):
        return relpath(path, start)

    def join(self, root, relative):
        return join(root, relative)

    def delete_batch_size(self):
        return 1

class S3Path(GenericPath):
    """valid objects are tuples (key, value) or (key, prefix)"""

    def __init__(self):
        self.s3 = get_thread_local_boto3()

    def enumerate_keys(self, root):
        bucket, prefix = root
        s3 = self.b3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            content = page.get('Contents', [])
            for item in content:
                k = item['Key']
                log.info('Detected key %s', k)
                yield (bucket, k)

    def delete(self, keys):
        keys = [{'Key': k[1]} for k in keys]
        bucket = keys[0][0]
        s3 = self.b3.client('s3')
        s3.delete_objects(Bucket=bucket, Delete={'Objects': keys})

    def relative(self, path, start):
        b1, p1 = path
        b2, p2 = start
        relative_path = p1.replace(p2, '')
        assert b1, relative_path

        return relpath(path, start)

    def join(self, root, relative):
        b1, p1 = root
        b2, p2 = relative
        joined = f'{p1}{p2}'
        assert b1 == b2
        return b1, joined

    def delete_batch_size(self):
        return 1000

class GenericCopier:

    def __init__(self):
        self.b3 = get_thread_local_boto3()

    def generic_copy(self, src, dst):
        s3 = self.boto3.client('s3')
        if type(src) == str:
            if type(dst) == tuple: #local to s3
                full_path = src
                bucket, key = dst
                return s3.upload(full_path, bucket, key)
        elif type(src) == tuple:
            src_bucket, src_key = src
            if type(dst) == tuple: #s3 to s3
                dst_bucket, dst_key = dst
                copy_src = {'Bucket': src_bucket, 'Key': src_key}
                return s3.copy_object(Bucket=dst_bucket, Key=dst_key, CopySource=copy_src)
            elif type(dst) == str:
                return s3.download_file(src_bucket, src_key, dst)

        raise Exception('unsupported')


    def execute_upload_synch(self):
        for local_file in self.enumerate_local(self.localroot):
            self.upload(local_file)

<<<<<<< Updated upstream
    def execute_upload_multithreaded(self):
        futures = []
        with ThreadPoolExecutor(max_workers=25) as tp:
            for local_file in self.enumerate_local(self.localroot):
                future = tp.submit(self.upload, local_file)
                futures.append(future)
=======
    def __init__(self):
        self.b3 = get_thread_local_boto3()
>>>>>>> Stashed changes

                while len(futures) > 100:
                    done, notdone = wait(futures, None, FIRST_COMPLETED)
                    for future in done:
                        future.result() # so any exception throws is visible
                    futures = list(notdone)

            done, notdone = wait(futures, None, ALL_COMPLETED)
            assert len(notdone) == 0
            for future in done:
                future.result()

    def execute_upload(self):
        self.execute_upload_multithreaded()

    def pre_download_file(self, Filename, **kwargs):
        cleansed = self.cleanse_invalid_chars(Filename)
        if cleansed != Filename:
            self.logger.info('Cleaned %s to %s', Filename, cleansed)
        directory = dirname(cleansed)
        self.logger.info('Creating directory %s', directory)
        makedirs(directory, 0o777, True)
        kwargs['Filename'] = cleansed
        return kwargs

    def to_components(self, path):
        drive, path = splitdrive(path)
        r = []
        while path != sep:
            path, tail = split(path)
            r.insert(0, tail)
        return drive, r

    def cleanse_invalid_chars(self, path):
        if sys.platform == 'win32':
            badchars = '/\\:"|?<>'
            invalid_names = 'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'

            drive, components = self.to_components(path)
            for idx, component in enumerate(components):
                for c in badchars:
                    component = component.replace(c, '_')

                for n in invalid_names:
                    if component == n or component.startswith('%s.' % n):
                        component = '_%s' % component
                components[idx] = component

            all = ['%s%s'% (drive, sep)]
            all.extend(components)
            j = join(*all)
            self.logger.info('Cleansed to %s', j)
            return j
        else:
            return path

    def s3_execute(self, method, kwargs):
        s3client = self.get_local_s3_client()
        self.logger.info('Executing %s', method)
        premethod = 'pre_%s' % method
        if hasattr(self, premethod):
            f = getattr(self, premethod)
            kwargs = f(**kwargs)
        f = getattr(s3client, method)
        r = f(**kwargs)
        return method, kwargs, r

    def execute_download(self):
        bucket, root = self.parse_s3_path(self.s3root)
        to_do = [('list_objects_v2', {'Bucket': bucket, 'Prefix': root})]
        futures = set()
        with ThreadPoolExecutor(max_workers=25) as tp:
            while to_do or futures:
                if to_do and len(futures) < 100:
                    method, kwargs = to_do.pop()
                    future = tp.submit(self.s3_execute, method, kwargs)
                    futures.add(future)
                else:
                    done, notdone = wait(futures, None, FIRST_COMPLETED)
                    futures = notdone
                    for future_done in done:
                        method, kwargs, r = future_done.result()
                        if method == 'list_objects_v2':
                            if r['IsTruncated']:
                                token = r['NextContinuationToken']
                                kwargs = {'Bucket': bucket, 'Prefix': root, 'ContinuationToken': token}
                                to_do.append(('list_objects_v2', kwargs))

                            if r['Contents']:
                                for entry in r['Contents']:
                                    key = entry['Key']
                                    relative = key.replace(root, '')
                                    parts = relative.split('/')
                                    join_params = [self.localroot] + parts
                                    local_file = join(*join_params)
                                    self.logger.info('Download to %s', local_file)
                                    to_do.append(('download_file', {'Bucket': bucket, 'Key': key, 'Filename': local_file}))

    def execute_s3_to_s3_copy(self, src, dst):
        bucket, root = self.parse_s3_path(src)
        dst_bucket, dst_prefix = self.parse_s3_path(dst)
        to_do = [('list_objects_v2', {'Bucket': bucket, 'Prefix': root})]
        futures = set()
        with ThreadPoolExecutor(max_workers=25) as tp:
            while to_do or futures:
                if to_do and len(futures) < 100:
                    method, kwargs = to_do.pop()
                    future = tp.submit(self.s3_execute, method, kwargs)
                    futures.add(future)
                else:
                    done, notdone = wait(futures, None, FIRST_COMPLETED)
                    futures = notdone
                    for future_done in done:
                        method, kwargs, r = future_done.result()
                        if method == 'list_objects_v2':
                            if r['IsTruncated']:
                                token = r['NextContinuationToken']
                                kwargs = {'Bucket': bucket, 'Prefix': root, 'ContinuationToken': token}
                                to_do.append(('list_objects_v2', kwargs))

                            if r['Contents']:
                                for entry in r['Contents']:
                                    key = entry['Key']
                                    relative = key.replace(root, '')
                                    abs_dst = f'{dst_prefix}{relative}'
                                    self.logger.info('Copy to %s:%s to %s:%s', bucket, key, dst_bucket, abs_dst)
                                    copy_source = {'Bucket': bucket, 'Key': key}
                                    to_do.append(('copy_object', {'Bucket': dst_bucket, 'Key': abs_dst, 'CopySource': copy_source}))



    def execute_rmtree(self):
        bucket, root = self.parse_s3_path(self.s3root)
        to_do = [('list_objects_v2', {'Bucket': bucket, 'Prefix': root})]
        to_delete_keys = []
        futures = set()
        with ThreadPoolExecutor(max_workers=25) as tp:
            while to_do or futures:
                if to_do and len(futures) < 100:
                    method, kwargs = to_do.pop()
                    future = tp.submit(self.s3_execute, method, kwargs)
                    futures.add(future)
                else:
                    done, notdone = wait(futures, None, FIRST_COMPLETED)
                    futures = notdone
                    for future_done in done:
                        method, kwargs, r = future_done.result()
                        if method == 'list_objects_v2':
                            if r['IsTruncated']:
                                token = r['NextContinuationToken']
                                kwargs = {'Bucket': bucket, 'Prefix': root, 'ContinuationToken': token}
                                to_do.append(('list_objects_v2', kwargs))

                            entries = r.get('Contents', [])
                            for entry in entries:
                                to_delete_keys.append({'Key': entry['Key']})
                                if len(to_delete_keys) == 1000:
                                    to_do.append(('delete_objects', {'Bucket': bucket, 'Delete': {'Objects': to_delete_keys}}))
                                    to_delete_keys = []
                        elif method == 'delete_objects':
                            errors = r.get('Errors', [])
                            if errors:
                                self.logger.error('%s errors', len(errors))
                                for error in errors:
                                    self.logger.error('Key: %(Key)s, VersionId: %(VersionId)s, Code: %(Code)s, Message: %(Message)s',
                                        error)
                                raise Exception('Could not delete Key: %(Key)s, VersionId: %(VersionId)s, Code: %(Code)s, Message: %(Message)s',
                                        errors[0])


            if to_delete_keys:
                future = tp.submit(self.s3_execute, 'delete_objects', {'Bucket': bucket, 'Delete': {'Objects': to_delete_keys}})
                done, notdone = wait([future], None, ALL_COMPLETED)
                assert len(notdone) == 0
                for f in done:
                    method, kwargs, r = f.result()
                    errors = r.get('Errors', [])
                    if errors:
                        self.logger.error('%s errors', len(errors))
                        for error in errors:
                            self.logger.error('Key: %(Key)s, VersionId: %(VersionId)s, Code: %(Code)s, Message: %(Message)s',
                                error)
                        raise Exception('Could not delete Key: %(Key)s, VersionId: %(VersionId)s, Code: %(Code)s, Message: %(Message)s',
                                errors[0])





def _is_s3(path):
    return path.startswith('s3://')


def _copytree_local_to_s3(src, dst):
    s3sh = S3ShutilEngine(src, dst)
    s3sh.execute_upload()

def _copytree_s3_to_local(src, dst):
    s3sh = S3ShutilEngine(s3root=src, localroot=dst)
    s3sh.execute_download()

def _copy_s3_to_s3(src, dst):
    s3sh = S3ShutilEngine(s3root=src, localroot=dst)
    s3sh.execute_s3_to_s3_copy(src, dst)

def copytree(src, dst):
    is_s3_src = _is_s3(src)
    is_s3_dst = _is_s3(dst)
    if not is_s3_src and not is_s3_dst:
        raise Exception(f'Unsupported: two non s3 paths {src} and {dst}')
    elif is_s3_src and not is_s3_dst:
        _copytree_s3_to_local(src, dst)
    elif not is_s3_src and is_s3_dst:
        _copytree_local_to_s3(src, dst)
    elif is_s3_src and is_s3_dst:
        _copy_s3_to_s3(src, dst)



def rmtree(src):
    if not _is_s3(src):
        raise Exception(f'Path {src} must start with s3://')

    s3sh = S3ShutilEngine(None, src)
    s3sh.execute_rmtree()


def move(src, dst):
    copytree(src, dst)
    if _is_s3(src):
        rmtree(src)
    else:
        shutil.rmtree(src)