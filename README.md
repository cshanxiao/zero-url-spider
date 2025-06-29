使用示例：
spider.exe --url https://hyper.ai --deep 3 --save_path ./urls.txt --total_time 300 --time_out 30 --start_from_index 1

deep: 爬取深度，默认3层
save_path: 存储文件路径，默认./urls.txt
total_time: 总执行时间,默认300秒
time_out: 打开网页超时时间，默认30秒
start_from_index: 是否从首页开始爬取，默认1，0：否 1：是

## 依赖库安装
### uv 使用示例
```shell
uv init
uv add requests
uv remove requests

uv add --group develop pandas
uv add --group production pandas
```
