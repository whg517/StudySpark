import os

import requests

url = 'http://repo.tendata.com.cn/repository/pypi-all/packages/54/85/0bef63668fb170888c1a2970ec897d4528d6072f32dee27653381a332642/Django-2.2-py3-none-any.whl'


# url = 'http://www.baidu.com'

def download_file(url, path, buffer=1024):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(os.path.join(path, local_filename), 'wb') as f:
            for chunk in r.iter_content(chunk_size=buffer):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    # f.flush()
    return local_filename


if __name__ == '__main__':
    download_file(url, '', buffer=8192)
