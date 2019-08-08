
## 打包

打包格式：

- wheel: bdist_wheel 二进制格式 (推荐)
- egg: bdist_egg 二进制格式
- tar.gz: 默认源码格式

```
python setup.py sdist --formats=zip,gztar
python setup.py bdist_wheel
python setup.py bdist_egg

```