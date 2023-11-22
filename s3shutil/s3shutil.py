from os.path import join, relpath, sep, dirname, splitdrive, split
from os import walk, makedirs, stat, sep
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import threading
import sys
import re
import itertools
import heapq

import boto3
import shutil


def parse_s3_path(s3path):
    """"returns a (bucket, fullpath) tuple from a s3://bucket/path/to/key string"""
    assert s3path[:5] == 's3://'
    parts = s3path.split('/')
    return parts[2], '/'.join(parts[3:])

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

    def enumerate_local_triple(self, root):                
        for directory, directories, files in walk(root):
            directories.sort()
            files.sort()
            for f in files:
                full_path = join(directory, f)
                s = stat(full_path)
                last_modified = s.st_mtime
                sz = s.st_size
                relative = relpath(full_path, root)
                slash_separate = relative.replace(sep, '/')
                yield slash_separate, last_modified, sz

    def enumerate_s3_triple(self, root):
        bucket, prefix = self.parse_s3_path(root)
        s3 = self.get_local_s3_client()
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get('Contents', [])
            for entry in contents:
                key = entry['Key']
                sz = entry['Size']
                last_modified = entry['LastModified']
                last_modified_ts = last_modified.timestamp()
                relative = key.replace(prefix, '')
                yield relative, last_modified_ts, sz



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

    def upload2(self, local_absolute, local_relative, local_root, s3_root):
        if local_relative is None:
            local_relative = relpath(local_absolute, local_root)
        elif local_absolute is None:
            local_absolute = join(local_root, local_relative)

        remote_relative = local_relative.replace(sep, '/')
        s3_path = '%s%s' % (s3_root, remote_relative)
        self.logger.info('Uploading %s to %s', local_absolute, s3_path)
        s3client = self.get_local_s3_client()
        bucket, key = self.parse_s3_path(s3_path)
        s3client.upload_file(local_absolute, bucket, key)


    def execute_upload_sync(self, local_root, s3_root):
        local_keys = self.enumerate_local_triple(local_root)
        s3_keys = self.enumerate_s3_triple(s3_root)

        local_tagged = map(lambda x:(x, 'src'), local_keys)
        s3_tagged = map(lambda x:(x, 'dst'), s3_keys)

        bucket, root = self.parse_s3_path(s3_root)

        merged = heapq.merge(local_tagged, s3_tagged)
        grouped = itertools.groupby(merged, lambda x:x[0][0])

        grouped = map(lambda x: (x[0], list(x[1])), grouped)
        #print(list(actions))
        actions_map = {
                    ('src', 'dst'): 'skip',
                    ('src',): 'copy',
                    ('dst',): 'delete'
                }

        to_delete_keys = []
        futures = []
        with ThreadPoolExecutor(max_workers=25) as tp:
            for key, group in grouped:
                actions_tuple = tuple((x[1] for x in group))
                action = actions_map[actions_tuple]
                print(f'{key} {action}')

                if action == 'skip':
                    continue
                elif action == 'copy':
                    local_file = join(local_root, key)
                    future = tp.submit(self.upload2, local_file, key, local_root, s3_root)
                    futures.append(future)
                elif action == 'delete':
                    to_del_key = f'{s3_root}{key}'
                    print(f'what is to {to_del_key}')
                    repl = f's3://{bucket}/'
                    print(f'repl is {repl}')
                    to_del_key = to_del_key.replace(repl, '')
                    print(f'after replacing {to_del_key}')

                    k = {'Key': to_del_key}
                    print(k)
                    to_delete_keys.append(k)
                    if len(to_delete_keys) == 1000:
                        future = tp.submit(self.s3_execute, 'delete_objects', {'Bucket': bucket, 'Delete': {'Objects': to_delete_keys}})
                        futures.append(future)
                        to_delete_keys = []

                while len(futures) > 100:
                    done, notdone = wait(futures, None, FIRST_COMPLETED)
                    for future in done:
                        future.result() # so any exception throws is visible
                    futures = list(notdone)

            if to_delete_keys:
                print(f'to delete keys {bucket} {to_delete_keys}')
                future = tp.submit(self.s3_execute, 'delete_objects', {'Bucket': bucket, 'Delete': {'Objects': to_delete_keys}})
                print('submitted')
                done, notdone = wait([future], None, ALL_COMPLETED)
                print(len(done))
                assert len(notdone) == 0
                for f in done:
                    method, kwargs, r = f.result()
                    print(f'done {method} {kwargs}')
                    errors = r.get('Errors', [])
                    print(errors)
                    if errors:
                        self.logger.error('%s errors', len(errors))
                        for error in errors:
                            self.logger.error('Key: %(Key)s, VersionId: %(VersionId)s, Code: %(Code)s, Message: %(Message)s',
                                              error)
                        raise Exception('Could not delete Key: %(Key)s, VersionId: %(VersionId)s, Code: %(Code)s, Message: %(Message)s',
                                        errors[0])
        

    def execute_upload_multithreaded(self):
        futures = []
        with ThreadPoolExecutor(max_workers=25) as tp:
            for local_file in self.enumerate_local(self.localroot):
                future = tp.submit(self.upload, local_file)
                futures.append(future)

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


    def to_components(self, path):
        drive, path = splitdrive(path)
        r = []
        while path != sep:
            path, tail = split(path)
            r.insert(0, tail)
        return drive, r


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


class BaseNode:

    def set_engine(self, engine):
        self.engine = engine

    def configure(self):
        pass

    def bootstrap(self):
        pass

    def on_result(self, method, kwargs, result):
        pass

    def flush(self):
        pass

class S3toS3Copier(BaseNode):

    def configure(self, src_bucket, src_root, dst_bucket, dst_root):
        self.src_bucket = src_bucket
        self.src_root = src_root
        self.dst_bucket = dst_bucket
        self.dst_root = dst_root

    def on_result(self, method, kwargs, result):
        if method != 'list_objects_v2':
            return []

        contents = result.get('Contents', [])
        for entry in contents:
            key = entry['Key']
            relative = key.replace(self.src_root, '')
            abs_dst = f'{self.dst_root}{relative}'
            copy_source = {'Bucket': self.src_bucket, 'Key': key}
            kwargs = {'Bucket': self.dst_bucket, 'Key': abs_dst, 'CopySource': copy_source}
            self.engine.enqueue_method('copy_object', kwargs)
        return []


class S3Downloader(BaseNode):

    def configure(self, bucket, s3_root, local_root):
        self.bucket = bucket
        self.s3_root = s3_root
        self.local_root = local_root

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

    def on_result(self, method, kwargs, result):
        if method != 'list_objects_v2':
            return []

        for entry in result.get('Contents', []):
            key = entry['Key']
            relative = key.replace(self.s3_root, '')
            parts = relative.split('/')
            join_params = [self.local_root] + parts
            local_file = join(*join_params)
            kwargs = {'Bucket': self.bucket, 'Key': key, 'Filename': local_file}

            cleansed = self.cleanse_invalid_chars(local_file)
            if cleansed != local_file:
                local_file = cleansed
                kwargs['Filename'] = local_file


            directory = dirname(local_file)
            makedirs(directory, 0o777, True)

            self.engine.enqueue_method('download_file', kwargs)

        return []


class S3Remover(BaseNode):

    def configure(self, bucket):
        self.bucket = bucket
        self.to_delete = []

    def flush(self):
        if self.to_delete:
            self.engine.enqueue_method('delete_objects', {'Bucket': self.bucket, 'Delete': {'Objects': self.to_delete}})
            self.to_delete = []

    def on_result(self, method, kwargs, result):
        if method != 'list_objects_v2':
            return []

        contents = result.get('Contents', [])
        for entry in contents:
            key = entry['Key']

            self.to_delete.append({'Key': key})
            if len(self.to_delete) == 1000:
                self.engine.enqueue_method('delete_objects', {'Bucket': self.bucket, 'Delete': {'Objects': self.to_delete}})
                self.to_delete = []

        return []


class ListObjectsV2(BaseNode):

    def configure(self, bucket, root, delimiter):
        self.bucket = bucket
        self.root = root
        self.delimiter = delimiter

    def bootstrap(self):
        kwargs = {'Bucket': self.bucket, 'Prefix': self.root}
        if self.delimiter is not None:
            kwargs['Delimiter'] = self.delimiter
        self.engine.enqueue_method('list_objects_v2', kwargs)

    def on_result(self, method, kwargs, result):
        if method != 'list_objects_v2':
            return

        if result['IsTruncated']:
            token = result['NextContinuationToken']
            kwargs = {'Bucket': self.bucket, 'Prefix': self.root, 'ContinuationToken': token}
            self.engine.enqueue_method('list_objects_v2', kwargs)

        prefixes = result.get('CommonPrefixes', [])
        for prefix_obj in prefixes:
            prefix = prefix_obj['Prefix']
            kwargs = {'Bucket': self.bucket, 'Prefix': self.root, 'Prefix': prefix}
            self.engine.enqueue_method('list_objects_v2', kwargs)

        contents = result.get('Contents', [])
        for entry in contents:
            key = entry['Key']
            yield key


class Engine:

    def __init__(self):
        self.queue = []

        self.workers = 25
        self.max_queue_size = 100
        self.thread_local = threading.local()

        self.log = logging.getLogger('eng')

    def enqueue_method(self, method_name, kwargs):
        self.log.info('Enqueueing %s %s', method_name, kwargs)
        self.queue.append((method_name, kwargs))

    def get_s3_client(self):
        client = getattr(self.thread_local, 's3', None)
        if client is None:
            client = boto3.Session().client('s3')
            setattr(self.thread_local, 's3', client)
        return client

    def invoke_s3_method(self, method, kwargs):
        self.log.info('Calling %s in thread %s', method, threading.get_native_id())
        s3 = self.get_s3_client()
        f = getattr(s3, method)
        r = f(**kwargs)
        return method, kwargs, r

    def do_all(self, nodes):
        self.log.info('do all')
        for node in nodes:
            node.set_engine(self)
            node.bootstrap()

        futures = set()
        with ThreadPoolExecutor(max_workers=self.workers) as tp:
            while futures or self.queue:
                self.log.info('Futures %s, queue %s, max_queue %s', len(futures), len(self.queue), self.max_queue_size)

                while self.queue and len(futures) < self.max_queue_size:
                    method, kwargs = self.queue.pop()
                    future = tp.submit(self.invoke_s3_method, method, kwargs)
                    futures.add(future)

                done, futures = wait(futures, None, FIRST_COMPLETED)
                for f in done:
                    method, kwargs, r = f.result()
                    for node in nodes:
                        for x in node.on_result(method, kwargs, r):
                            yield x

            self.log.info('Before flushing, queue size is %s, futures %s', len(self.queue), len(futures))
            for node in nodes:
                node.flush()

            self.log.info('After flushing, queue size is %s, futures %s', len(self.queue), len(futures))
            while self.queue:
                method, kwargs = self.queue.pop()
                future = tp.submit(self.invoke_s3_method, method, kwargs)
                futures.add(future)

            done, futures = wait(futures, None, FIRST_COMPLETED)
            for f in done:
                method, kwargs, r = f.result()
                for node in nodes:
                    for x in node.on_result(method, kwargs, r):
                        yield x




def _is_s3(path):
    return path.startswith('s3://')


def _copytree_local_to_s3(src, dst):
    s3sh = S3ShutilEngine(src, dst)
    s3sh.execute_upload()

def _copytree_s3_to_local(src, dst):
    src_bucket, src_prefix = parse_s3_path(src)

    eng = Engine()
    list_obj = ListObjectsV2()

    list_obj.configure(src_bucket, src_prefix, None)
    list_obj.set_engine(eng)

    s3 = S3Downloader()
    s3.configure(src_bucket, src_prefix, dst)
    s3.set_engine(eng)

    list(eng.do_all([list_obj, s3]))

def _copy_s3_to_s3(src, dst):
    src_bucket, src_prefix = parse_s3_path(src)
    dst_bucket, dst_prefix = parse_s3_path(dst)

    eng = Engine()
    list_obj = ListObjectsV2()

    list_obj.configure(src_bucket, src_prefix, None)
    list_obj.set_engine(eng)

    s3 = S3toS3Copier()
    s3.configure(src_bucket, src_prefix, dst_bucket, dst_prefix)
    s3.set_engine(eng)

    list(eng.do_all([list_obj, s3]))


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

    src_bucket, src_prefix = parse_s3_path(src)

    eng = Engine()
    rm = S3Remover()
    rm.configure(src_bucket)
    rm.set_engine(eng)

    list_obj = ListObjectsV2()
    list_obj.configure(src_bucket, src_prefix, None)
    list_obj.set_engine(eng)

    list(eng.do_all([list_obj, rm]))


def move(src, dst):
    copytree(src, dst)
    if _is_s3(src):
        rmtree(src)
    else:
        shutil.rmtree(src)