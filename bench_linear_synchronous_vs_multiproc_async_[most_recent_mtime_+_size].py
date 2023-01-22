import asyncio
import datetime
import os
import time
from aiomultiprocess import Pool
import chunk_handler
import prescan


def linear_synchronous_scan():
    print(f'[linear synchronous benchmark]')
    t = time.perf_counter()
    r = []
    for dirName, subdirname, filelist in os.walk('D:\\'):
        for fname in filelist:
            fp = os.path.join(dirName, fname)
            mt = os.path.getmtime(fp)
            sz = os.path.getsize(fp)
            r.append([fp, mt, sz])
    fp = []
    mt = []
    sz = []
    for result in r:
        fp.append(result[0])
        mt.append(result[1])
        sz.append(result[2])
    max_mt = max(mt)
    idx_max_mt = mt.index(max_mt)
    print('    [most recently modified]', datetime.datetime.fromtimestamp(max(mt)))
    print(f'    [file] {fp[idx_max_mt]}')
    print(f'    [size] {sz[idx_max_mt]} bytes')
    print(f'    [linear synchronous time] {time.perf_counter() - t}')


def pre_scan_handler():
    scan_results = prescan.scan(path='D:\\')
    _files = scan_results[0]
    return _files


async def get_mtime(f):
    return os.path.getmtime(f)


async def get_sz(f):
    return os.path.getsize(f)


async def async_mtime(f: list):
    return [f, await get_mtime(f), await get_sz(f)]


async def entry_point(chunks: list, **kwargs) -> list:
    return [await async_mtime(f) for f in chunks]


async def main(_chunks):
    async with Pool() as pool:
        return await pool.map(entry_point, _chunks, {})


if __name__ == '__main__':
    print('\n[benchmarking]  -> [find most recent modified time + get file sizes] ..')

    print('')
    linear_synchronous_scan()
    print('')

    print(f'[multi-processed async benchmark]')
    t = time.perf_counter()
    files = pre_scan_handler()
    chunks = chunk_handler.chunk_data(files, chunk_size=128)
    r = asyncio.run(main(chunks))
    r = chunk_handler.un_chunk_data(r, depth=1)
    fp = []
    mt = []
    sz = []
    for result in r:
        fp.append(result[0])
        mt.append(result[1])
        sz.append(result[2])
    max_mt = max(mt)
    idx_max_mt = mt.index(max_mt)
    print('    [most recently modified]', datetime.datetime.fromtimestamp(max(mt)))
    print(f'    [file] {fp[idx_max_mt]}')
    print(f'    [size] {sz[idx_max_mt]} bytes')
    print(f'    [multi-processed async time] {time.perf_counter() - t}')
