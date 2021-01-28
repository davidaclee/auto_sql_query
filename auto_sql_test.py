# %%
import re
import jsonify
import http.client
import hashlib
import urllib
import random
import json


sql = """
select
    dt
    ,busi_type
    ,app_id
    ,app_name
    ,sum(expose_nums) expose_nums
    ,sum(click_nums) click_nums
    ,sum(pay_nums) pay_nums
    ,sum(amount) amount
    ,sum(click_nums)/sum(expose_nums) ctr
from tmp_db.ods_tbl
where dt = 20200923
group by dt
,busi_type
,app_id
,app_name
;
"""

sql_col_dict = {
    "dt": {"col": "dt", "comment": "日期", "dtype": "string"},
    "imei": {"col": "imei", "comment": "imei", "dtype": "string"},
    "app_id": {"col": "app_id", "comment": "应用ID", "dtype": "string"},
    "app_name": {"col": "app_name", "comment": "应用名称", "dtype": "string"},
    "busi_type": {"col": "busi_type", "comment": "业务类型", "dtype": "string"},
    "prod_id": {"col": "prod_id", "comment": "商品ID", "dtype": "string"},
    "click_nums": {"col": "down_nums", "comment": "点击量", "dtype": "bigint"},
    "pay_nums": {"col": "pay_nums", "comment": "购买量", "dtype": "bigint"},
    "amount": {"col": "amount", "comment": "金额", "dtype": "bigint"},
    "expose_nums":{"col": "expose_nums", "comment": "曝光量", "dtype": "bigint"},
    "cvr":{"col": "cvr", "comment": "转化率", "dtype": "double"},
    "ctr":{"col": "ctr", "comment": "点击率", "dtype": "double"}
}
sql = sql.lower()
col_list_tmp = sql.replace('\n', '').replace('select', '').replace('from', '?').replace('where', '?')
col_list_tmp = col_list_tmp.split('?')[0]

# 替换调用函数时括号里的逗号
p = re.compile('\.*?[(](.*?)[)]', re.S)
# print(re.findall(p, col_list_tmp))
col_list_tmp = p.sub('', col_list_tmp)

# # logging.debug('特殊字符处理' + col_list_tmp)
col_list_len = len(col_list_tmp.split(','))

sql_col_info = []

for i in range(col_list_len):
    tmp_col = col_list_tmp.split(',')[i].strip()
    if len(tmp_col.split(' ')) > 0:
        col = tmp_col.split(' ')[-1]
    else:
        col = tmp_col

    if col in sql_col_dict.keys():
        info = sql_col_dict[col]
        info.update(id=i)
    else:
        # comment = baidu_translate(col)
        comment = '未知'
        info = {"col": col, "comment": comment, "dtype":"string","id": i}
    sql_col_info.append(info)
print('Analysis Result:', sql_col_info)
# print('Jsonify Result:', jsonify(sql_col_info))
# %%
db_name = 'test_db'
tbl_name = 'test_tbl'
tbl_comment = 'test_comment'
hive_text = ''
mysql_text = ''
for i in range(len(sql_col_info)):
    col_info = sql_col_info[i]
    if col_info['dtype'] == 'string':
        mysql_text += '{col} varchar(40) comment "{comment}",\n'.format(**col_info)
    else:
        mysql_text += '{col} {dtype} comment "{comment}",\n'.format(**col_info)
    if i == len(sql_col_info) - 1:
        hive_text += '{col} {dtype} comment "{comment}"\n'.format(**col_info)
    else:
        hive_text += '{col} {dtype} comment "{comment}",\n'.format(**col_info)

hive_head = 'create table ' + db_name + '.'+ tbl_name + '\n('
hive_foot = ')' + ' comment "' + tbl_comment + '"\nPARTITIONED BY (dayno bigint COMMENT "日期")\nSTORED AS ORCFILE;'

mysql_head = "create table " + tbl_name + '\n(\n' + 'id bigint unsigned NOT NULL AUTO_INCREMENT COMMENT "主键ID",\ndt date NOT NULL DEFAULT "0000-00-00" COMMENT "日期,时间格式日期",\n'
mysql_foot = 'PRIMARY KEY (`id`,`dt`),\nKEY `idx_dt` (`dt`)\n) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT="' + tbl_comment + '"\nPARTITION BY RANGE (TO_DAYS(dt))\n(PARTITION pmin VALUES LESS THAN (737850) ENGINE = InnoDB,\nPARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB);'



hive_query = hive_head + '\n' + hive_text + hive_foot
mysql_query = mysql_head + mysql_text + mysql_foot

all_query = '<b># Hive 建表语句</b>' + '\n' + hive_query + '\n\n' + '<b># MySQL 建表语句</b>' + '\n' + mysql_query

tbl_query = {
    'all_query': all_query
}

print(tbl_query)
# %%
