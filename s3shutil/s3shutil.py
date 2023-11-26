from os.path import join, relpath, sep, dirname, splitdrive, split
from os import walk, makedirs, stat, sep
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import threading
import sys
import itertools
import heapq
import boto3
import functools
import shutil

log = logging.getLogger('eng')

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

def parse_s3_path(s3path):
    """"returns a (bucket, fullpath) tuple from a s3://bucket/path/to/key string"""
    assert s3path[:5] == 's3://'
    parts = s3path.split('/')
    return parts[2], '/'.join(parts[3:])




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


class Engine:

    def __init__(self):
        self.b3 = ThreadLocalBoto3()

    def _list_objects_generator(self, src_root):
        for directory, dirs, files in walk(src_root):
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



    def _list_keys(self, bucket, prefix):
        s3 = self.b3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            content = page.get('Contents', [])
            for item in content:
                k = item['Key']
                log.info('Detected key %s', k)
                yield k

    def _upload(self, args):
        local_file, bucket, key = args
        s3= self.b3._get_client('s3')
        log.info(f'Uploading %s to s3://%s/%s', local_file, bucket, key)
        r = s3.upload_file(local_file, bucket, key)

    def _copy(self, args):
        src_bucket, src_key, dst_bucket, dst_key = args
        s3= self.b3._get_client('s3')
        log.info(f'Copying s3://%s/%s to s3://%s/%s', src_bucket, src_key, dst_bucket, dst_key)
        copy_src = {'Bucket': src_bucket, 'Key': src_key}
        r = s3.copy_object(Bucket=dst_bucket, Key=dst_key, CopySource=copy_src)


    def _download(self, args):
        bucket, key, src_prefix, src_root = args
        log.info('Download s3://%s/%s (root is %s) to %s', bucket, key, src_prefix, src_root)
        relative = key.replace(src_prefix, '')
        log.info('Relative %s', relative)
        parts = relative.split('/')
        log.info('Parts %s', parts)
        extend = join(src_root, *parts)
        log.info('Extend %s', extend)
        directory = dirname(extend)
        log.info('Makedir %s', directory)
        makedirs(directory, 0o777, exist_ok=True)
        s3 = self.b3._get_client('s3')
        log.info('download_file bucket %s key %s extend %s', bucket, key, extend)
        s3.download_file(bucket, key, extend)
        log.info('Downloaded')

    def _delete_keys(self, args):
        bucket, keys = args
        log.info('Deleting from bucket %s, number of keys %s', bucket, len(keys))
        for i, key in enumerate(keys):
            log.info('Key %s is %s', i, key)
        keys = [{'Key': k} for k in keys]
        self.b3.client('s3').delete_objects(Bucket=bucket, Delete={'Objects': keys})

    def tp(self):
        from concurrent.futures import ThreadPoolExecutor
        return ThreadPoolExecutor(max_workers=25, thread_name_prefix='tp')

    def map_and_collect(self, f, iterator):
        with self.tp() as tp:
            rs = tp.map(f, iterator)
            functools.reduce(lambda x, y: None, rs, None)


    def _upload_many(self, src_root, files_iterator, dest_bucket, dest_prefix, tp):
        args_f = lambda file: (file, dest_bucket, f'{dest_prefix}{relpath(file, src_root).replace(sep, "/")}')
        args_it = map(args_f, files_iterator)
        tp.map(self._upload, args_it)

    def upload_tree(self, src_root, dest_bucket, dest_prefix):
        files = self._list_objects_generator(src_root)
        with self.tp() as tp:
            self._upload_many(src_root, files, dest_bucket, dest_prefix, tp)

    def upload_sync_tree(self, src_root, dest_bucket, dest_prefix):
        local_keys = self._list_objects_generator(src_root)
        rel_local_keys = map(lambda x: relpath(x, src_root), local_keys)

        s3_keys = self._list_keys(dest_bucket, dest_prefix)
        rel_s3_keys = map(lambda x: x.replace(dest_prefix, ''), s3_keys)

        local_tagged = map(lambda x:(x, 'src'), rel_local_keys)
        s3_tagged = map(lambda x:(x, 'dst'), rel_s3_keys)

        merged = heapq.merge(local_tagged, s3_tagged, key=lambda x: x[0])
        grouped = itertools.groupby(merged, lambda x:x[0])

        grouped = map(lambda x: (x[0], tuple((y[1] for y in x[1]))), grouped)
        actions = {
            ('src', 'dst'): 'skip',
            ('src',): 'upload',
            ('dst',): 'delete'
        }

        with_action = map(lambda x: (x[0], actions[x[1]]), grouped)
        without_skip = filter(lambda x:x[1] != 'skip', with_action)
        t1, t2 = itertools.tee(without_skip)

        uploads = filter(lambda x:x[1] == 'upload', t1)
        upload_keys = map(lambda x:x[0], uploads)
        absolute = map(lambda x: join(src_root, x), upload_keys)

        deletes = filter(lambda x:x[1] == 'delete', t2)
        delete_keys = map(lambda x:x[0], deletes)
        del_keys_rel_to_root = map(lambda x: f'{dest_prefix}{x}', delete_keys)
        batched_dels = itertools_batched(del_keys_rel_to_root, 1000)
        batched_dels_args = map(lambda x: (dest_bucket, x), batched_dels)

        with self.tp() as tp:
            self._upload_many(src_root, absolute, dest_bucket, dest_prefix, tp)
            tp.map(self._delete_keys, batched_dels_args)

    def download_tree(self, src_bucket, src_prefix, src_root):
        keys = self._list_keys(src_bucket, src_prefix)
        params = map(lambda x: (src_bucket, x, src_prefix, src_root), keys)
        self.map_and_collect(self._download, params)


    def copy_tree(self, src_bucket, src_prefix, dst_bucket, dst_prefix):
        keys = self._list_keys(src_bucket, src_prefix)
        dst_key_f = lambda x: x.replace(src_prefix, dst_prefix)
        args_f = lambda x: (src_bucket, x, dst_bucket, dst_key_f(x))
        params = map(lambda x: args_f(x), keys)
        self.map_and_collect(self._copy, params)

    def delete_s3_tree(self, s3_bucket, s3_prefix):
        keys = self._list_keys(s3_bucket, s3_prefix)
        batched = itertools_batched(keys, 1000)
        params = map(lambda x: (s3_bucket, x), batched)

        self.map_and_collect(self._delete_keys, params)




def rmtree(src):
    if not _is_s3(src):
        raise Exception(f'Path {src} must start with s3://')

    src_bucket, src_prefix = parse_s3_path(src)
    e = Engine()
    e.delete_s3_tree(src_bucket, src_prefix)


def _is_s3(path):
    return path.startswith('s3://')


def _copytree_local_to_s3(src, dst):
    dst_bucket, dst_prefix = parse_s3_path(dst)
    e = Engine()
    e.upload_tree(src, dst_bucket, dst_prefix)

def _copytree_s3_to_local(src, dst):
    src_bucket, src_prefix = parse_s3_path(src)
    e = Engine()
    log.info('_copytree_s3_to_local s3://%s/%s to %s', src_bucket, src_prefix, dst)
    e.download_tree(src_bucket, src_prefix, dst)

def _copy_s3_to_s3(src, dst):
    src_bucket, src_prefix = parse_s3_path(src)
    dst_bucket, dst_prefix = parse_s3_path(dst)
    e = Engine()
    e.copy_tree(src_bucket, src_prefix, dst_bucket, dst_prefix)


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

def upload_sync(local_src, dst):
    dst_bucket, dst_prefix = parse_s3_path(dst)

    e = Engine()
    e.upload_sync_tree(local_src, dst_bucket, dst_prefix)



def move(src, dst):
    copytree(src, dst)
    if _is_s3(src):
        rmtree(src)
    else:
        shutil.rmtree(src)

def main():

    eng = logging.getLogger('eng')
    logging.getLogger().setLevel(logging.WARN)
    eng.setLevel(logging.DEBUG)
    logging.basicConfig(stream=sys.stdout, format='%(levelname)s:%(threadName)s:%(message)s')

    bucket = 's3shutil-test-bucket-vvlv7xebuek'
    prefix = '1/2/4/'
    local_root = '/Users/aworms/src/s3shutil/s3shutil/sample'

    e = Engine()
    e.upload_tree(local_root, bucket, prefix)
    e.upload_sync_tree(local_root, bucket, '1/2/3/')

    e.delete_s3_tree(bucket, f'{prefix}/')

if __name__=='__main__':
    main()