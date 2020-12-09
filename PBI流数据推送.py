import requests
import numpy as np
import json
import time

import apscheduler.schedulers.blocking

""""
PowerBI提供API创建流数据集的方法，定义流数据名称和数据类型后生成url，使用POST方法推送即可

如：在PowerBI中创建时间和销售额两个数据，数据类型为日期时间和数字
URL:
"https://api.powerbi.com/beta/564e70f4-345d-481f-9a36-a953dc5fb1fc/datasets/921872a4-0cd3-4dee-9113-64ab72c69bbe/rows?key=kmG32rctsiEwu6sY73yLNJnrMCL1IO3n1zvbT1NOPMHj1pxVn4bCR3sbkQd0TpLaAdxXg0ZPdemF8DWJ4d3tEQ%3D%3D"

方法:POST
header "Content-Type: application/json"

POST数据格式示例：
[{"时间" :"2020-12-09T06:29:32.011Z","销售额" :98.6}]

"""


def push():
    url = "https://api.powerbi.com/beta/564e70f4-345d-481f-9a36-a953dc5fb1fc/datasets/921872a4-0cd3-4dee-9113" \
          "-64ab72c69bbe/rows?key" \
          "=kmG32rctsiEwu6sY73yLNJnrMCL1IO3n1zvbT1NOPMHj1pxVn4bCR3sbkQd0TpLaAdxXg0ZPdemF8DWJ4d3tEQ%3D%3D "
    headers = {'Content-Type': 'application/json'}

    # 时间，使用当前系统时间
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 100-10000随机数，作为销售额数据
    num = np.random.randint(100, 10000, 1)
    # [和]作为起始和结尾提示，否则会400状态，返回错误提示
    post_data = "[" + json.dumps({"时间": t, "销售额": int(num[0])}) + "]"

    # post请求
    requests.post(url=url, data=post_data, headers=headers)

    print("推送数据:", num[0])

    # 查看请求状态是否200
    # print(r.status_code, r.content)


# 每3秒推送一次
def job():
    scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
    scheduler.add_job(push, 'interval', seconds=3, id="test")
    scheduler.start()


if __name__ == '__main__':
    job()
