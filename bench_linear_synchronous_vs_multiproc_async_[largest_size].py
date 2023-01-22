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
            sz = os.path.getsize(fp)
            r.append([fp, sz])
    fp = []
    sz = []
    for result in r:
        fp.append(result[0])
        sz.append(result[1])
    max_sz = max(sz)
    idx_max_sz = sz.index(max_sz)
    print(f'    [file] {fp[idx_max_sz]}')
    print(f'    [size] {sz[idx_max_sz]} bytes')
    print(f'    [linear synchronous time] {time.perf_counter() - t}')


def pre_scan_handler():
    scan_results = prescan.scan(path='D:\\')
    _files = scan_results[0]
    return _files


async def get_sz(f):
    return [f, os.path.getsize(f)]


async def entry_point(chunks: list, **kwargs) -> list:
    return [await get_sz(f) for f in chunks]


async def main(_chunks):
    async with Pool() as pool:
        return await pool.map(entry_point, _chunks, {})


if __name__ == '__main__':
    print('\n[benchmarking]  -> [find largest size] ..')

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
    sz = []
    for result in r:
        fp.append(result[0])
        sz.append(result[1])
    max_sz = max(sz)
    idx_max_sz = sz.index(max_sz)
    print(f'    [file] {fp[idx_max_sz]}')
    print(f'    [size] {sz[idx_max_sz]} bytes')
    print(f'    [multi-processed async time] {time.perf_counter() - t}')
