import os.path
from os.path import join, relpath, dirname, basename, isdir
from os import walk, unlink, makedirs

import logging
import threading
import itertools
import heapq
import boto3
import functools
import shutil

log = logging.getLogger('s3shutil')
debug_iterators = False

try:
    from itertools import batched as itertools_batched
except:
    def itertools_batched(it, batch_size):
        current_batch = []
        for x in it:
            current_batch.append(x)
            if len(current_batch) == batch_size:
                yield current_batch
                current_batch = []

        if current_batch:
            yield current_batch

def debug_iterator(name, it):
    if not debug_iterators:
        return it
    copy1, copy2 = itertools.tee(it)
    l = list(copy1)
    log.info('DEBUG, iterator %s of length %s', name, len(l))
    for i, x in enumerate(l):
        log.info(' DEBUG %s: %s', i, x)

    return copy2

def parse_s3_path(s3path):
    """"returns a (bucket, fullpath) tuple from a s3://bucket/path/to/key string"""
    assert s3path[:5] == 's3://'
    parts = s3path.split('/')
    bucket = parts[2]
    path = '/'.join(parts[3:])
    return s3_path((bucket, path))

def parse_fs_path(fspath):
    return fs_path(fspath)

def _is_s3(path):
    return path.startswith('s3://')

def generic_parse_path(path):
    if _is_s3(path):
        return parse_s3_path(path)
    else:
        return parse_fs_path(path)


class ThreadLocalBoto3:

    def __init__(self):
        self.thread_local = threading.local()

    def _get_thread_local(self, name, factory_func):
        v = getattr(self.thread_local, name, None)
        if v is None:
            v = factory_func()
            setattr(self.thread_local, name, v)

        return v

    def _get_session(self):
        return self._get_thread_local('session', boto3.Session)

    def _get_client(self, service):
        return self._get_thread_local(service, lambda: self._get_session().client(service))

    def client(self, service):
        return self._get_client(service)

_thread_local_boto3_singleton = None

def get_thread_local_boto3():
    global _thread_local_boto3_singleton
    if _thread_local_boto3_singleton is None:
        _thread_local_boto3_singleton = ThreadLocalBoto3()

    return _thread_local_boto3_singleton


class generic_path:

    def relative(self, path, start):
        pass

    def join(self, root, relative):
        pass

    def delete_batch_size(self):
        return 1

    def get_type(self):
        pass

    def get_path(self):
        pass

    def filename(self):
        pass


class fs_path(generic_path):
    """valid objects are strings"""

    def __init__(self, path):
        self.path = path

    def get_type(self):
        return 'fs'

    def relative(self, start):
        return relpath(self.path, start.path)

    def join(self, relative):
        return fs_path(join(self.path, relative))

    def delete_batch_size(self):
        return 1

    def get_type(self):
        return 'fs'

    def get_path(self):
        return self.path

    def filename(self):
        return basename(self.path)

    def __str__(self):
        return f'fs://{self.path}'

    __repr__ = __str__


class s3_path(generic_path):

    def __init__(self, path):
        self.bucket, self.path = path

    def get_type(self):
        return 's3'

    def relative(self, start):
        assert self.bucket == start.bucket
        return self.path.replace(start.path, '')

    def join(self, relative):
        p = (self.bucket, f'{self.path}{relative}')
        return s3_path(p)

    def delete_batch_size(self):
        return 1000

    def get_type(self):
        return 's3'

    def get_path(self):
        return self.bucket, self.path

    def filename(self):
        return self.path.split('/')[-1]

    def __str__(self):
        return f's3://{self.bucket}/{self.path}'

    __repr__ = __str__


class GenericOps:

    def __init__(self):
        self.b3 = get_thread_local_boto3()
        self.log = logging.getLogger('s3shutil.ops')

    def generic_list(self, src):
        self.log.info('generic_list src=%s', src)
        if src.get_type() == 's3':
            bucket, prefix = src.get_path()
            s3 = self.b3.client('s3')
            paginator = s3.get_paginator('list_objects_v2')
            self.log.info('paginate Bucket=%s, Prefix=%s', bucket, prefix)
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for entry in page.get('Contents', []):
                    key = entry['Key']
                    obj = s3_path((bucket, key))
                    self.log.debug('Found %s', obj)
                    yield obj

        elif src.get_type() == 'fs':
            path = src.get_path()
            self.log.info('os.walk %s', path)
            for directory, dirs, files in walk(path):
                dirs.sort()
                files.sort()
                for f in files:
                    fp = join(directory, f)
                    obj = fs_path(fp)
                    self.log.info('Found %s', obj)
                    yield obj

        else:
            raise Exception(f'unsupported type {src.get_type()}')

    def rm_s3(self, keys):
        bucket, key = keys[0].get_path()
        self.log.info('rm %s keys, the first one is %s:%s', len(keys), bucket, key)
        keys = [{'Key': k.get_path()[1]} for k in keys]

        if self.log.isEnabledFor(logging.DEBUG):
            for i, key in enumerate(keys):
                self.log.debug('key %s is %s', i, key)

        s3 = self.b3.client('s3')
        r = s3.delete_objects(Bucket=bucket, Delete={'Objects': keys})
        return r

    def rm_fs(self, keys):
        assert len(keys) == 1 # in fs the batch size is 1
        self.log.info('rm 1 key %s', keys[0])
        for key in keys:
            unlink(key)

    def rm_generic(self, keys):
        assert len(keys) > 0
        tp = keys[0].get_type()
        assert tp in ('fs', 's3')
        if tp == 'fs':
            self.rm_fs(keys)
        elif tp == 's3':
            self.rm_s3(keys)

    def generic_copy(self, src, dst):
        s3 = self.b3.client('s3')
        if type(src) == fs_path:
            if type(dst) == s3_path: #local to s3
                full_path = src.get_path()
                bucket, key = dst.get_path()
                self.log.info('uploading %s to %s:%s', full_path, bucket, key)
                r = s3.upload_file(full_path, bucket, key)
                return r
        elif type(src) == s3_path:
            src_bucket, src_key = src.get_path()
            if type(dst) == s3_path: #s3 to s3
                dst_bucket, dst_key = dst.get_path()
                copy_src = {'Bucket': src_bucket, 'Key': src_key}
                r = s3.copy_object(Bucket=dst_bucket, Key=dst_key, CopySource=copy_src)
                self.log.info('copy %s:%s to %s:%s', src_bucket, src_key, dst_bucket, dst_key)
                result = r['CopyObjectResult']
                self.log.info('Result %s', result)
                return r
            elif type(dst) == fs_path:
                path = dst.get_path()
                directory = dirname(path)
                makedirs(directory, 0o777, exist_ok=True)
                r = s3.download_file(src_bucket, src_key, path)
                self.log.info('downloaded %s:%s to %s', src_bucket, src_key, path)
                return r

        raise Exception('unsupported')

class Engine:

    def __init__(self):
        self.b3 = ThreadLocalBoto3()
        self.generic_ops = GenericOps()
        self.log = logging.getLogger('s3shutil.engine')

    def empty_iterator(self):
        return []

    def tp(self):
        from concurrent.futures import ThreadPoolExecutor
        return ThreadPoolExecutor(max_workers=25, thread_name_prefix='tp')

    def map_and_collect(self, f, iterator):
        with self.tp() as tp:
            rs = tp.map(f, iterator)
            self.exhaust_iterator(rs)

    def exhaust_iterator(self, it):
        self.log.info('waiting for results')
        ones = map(lambda x: 1, it)
        count = functools.reduce(lambda x, y: x+y, ones, 0)
        self.log.info('Iterator count = %s', count)
        return count

    def cp(self, args):
        src, dst = args
        return self.generic_ops.generic_copy(src, dst)

    def generic_copy_file(self, src, dst):
        self.generic_ops.generic_copy(src, dst)

    def generic_copy_tree(self, src_root, dst_root, sync=False):
        self.log.info('generic copy tree %s, %s, sync=%s', src_root, dst_root, sync)
        assert issubclass(type(src_root), generic_path) or src_root is None
        assert issubclass(type(dst_root), generic_path)

        if src_root is None:
            self.log.info('Src is root, we are deleting dst')
            src_keys = self.empty_iterator()
        else:
            src_keys = self.generic_ops.generic_list(src_root)

        if sync:
            dst_keys = self.generic_ops.generic_list(dst_root)
        else:
            dst_keys = self.empty_iterator()

        src_keys = debug_iterator('Source Keys', src_keys)
        dst_keys = debug_iterator('Dest Keys  ', dst_keys)

        rel_src_keys = map(lambda src_key: src_key.relative(src_root), src_keys)
        rel_dst_keys = map(lambda dst_key: dst_key.relative(dst_root), dst_keys)

        src_tagged = map(lambda x:(x, 'src'), rel_src_keys)
        dst_tagged = map(lambda x:(x, 'dst'), rel_dst_keys)

        merged = heapq.merge(src_tagged, dst_tagged, key=lambda x: x[0])
        grouped = itertools.groupby(merged, lambda x:x[0])

        grouped = map(lambda x: (x[0], tuple((y[1] for y in x[1]))), grouped)
        actions = {
            ('src', 'dst'): 'skip',
            ('src',): 'copy',
            ('dst',): 'delete'
        }

        with_action = map(lambda x: (x[0], actions[x[1]]), grouped)
        with_action = debug_iterator('With action', with_action)

        without_skip = filter(lambda x:x[1] != 'skip', with_action)

        without_skip = debug_iterator('Without skip', without_skip)

        t1, t2 = itertools.tee(without_skip)

        cp = filter(lambda x:x[1] == 'copy', t1)

        cp_keys = map(lambda x:x[0], cp)
        cp_params = map(lambda x: (src_root.join(x), dst_root.join(x)), cp_keys)
        cp_params = debug_iterator('Upload params', cp_params)


        deletes = filter(lambda x:x[1] == 'delete', t2)
        delete_keys = map(lambda x:x[0], deletes)
        delete_keys = debug_iterator('Deletes', delete_keys)
        delete_absolute = map(dst_root.join, delete_keys)
        delete_batched = itertools_batched(delete_absolute, 1000)
        delete_batched = debug_iterator('delete batched', delete_batched)

        with self.tp() as tp:
            self.log.info('submitting deletes')
            del_r = tp.map(self.generic_ops.rm_generic, delete_batched)
            self.log.info('submitting copies')
            cp_r = tp.map(self.cp, cp_params)
            self.exhaust_iterator(itertools.chain(cp_r, del_r))


def tree_sync(src, dst):
    src_path = generic_parse_path(src)
    dst_path = generic_parse_path(dst)

    e = Engine()
    e.generic_copy_tree(src_path, dst_path, sync=True)


def tree_copy(src, dst):
    src_path = generic_parse_path(src)
    dst_path = generic_parse_path(dst)

    e = Engine()
    e.generic_copy_tree(src_path, dst_path, sync=False)


def tree_rm(src):
    src_path = generic_parse_path(src)

    e = Engine()
    e.generic_copy_tree(None, src_path, sync=True)


def tree_move(src, dst):
    tree_copy(src, dst)
    if _is_s3(src):
        tree_rm(src)
    else:
        shutil.rmtree(src)


def copyfile(src, dst):
    src_path = generic_parse_path(src)
    dst_path = generic_parse_path(dst)

    e = Engine()
    e.generic_copy_file(src_path, dst_path)

def copy(src, dst):
    src_path = generic_parse_path(src)
    dst_path = generic_parse_path(dst)

    basename = src_path.filename()

    if dst_path.get_type() == 's3':
        bucket, path = dst_path.get_path()
        if path[-1] == '/':
            dst_path = s3_path((bucket, f'{path}{basename}'))

    elif dst_path.get_type() == 'fs':
        path = dst_path.get_path()
        if isdir(path):
            joined = join(path, basename)
            dst_path = fs_path(joined)

    e = Engine()
    e.generic_copy_file(src_path, dst_path)

def disk_usage(src):
    pass

copy2 = copy

rmtree = tree_rm
copytree = tree_copy
move = tree_move
