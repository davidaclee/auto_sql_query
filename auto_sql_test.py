#%%
import re
db_name = 'ad_tmp'
tbl_name = 'lzc_tmp_ads_srh_test_inc_d'
tbl_comment = '测试表'

# %%
import logging

logging.basicConfig(level=logging.DEBUG,format = '%(asctime)s - %(levelname)s - %(message)s')

sql = """
select
    dayno
    ,imei
    ,sc
    ,sum(expose_nums) expose_nums
    ,sum(evt_nums) evt_nums
    ,sum(acc_nums) acc_nums
    ,sum(acc_cost) acc_cost
from ad_tmp.lzc_test_table
where dayno = 20200820
group by dayno, imei, sc
;
"""

def get_sql_col_info(sql):
    sql = sql.lower()
    col_list_tmp = sql.replace('\n', '').replace('select', '').replace('from', '?').replace('where', '?')
    col_list_tmp = col_list_tmp.split('?')[0]

    # logging.debug('特殊字符处理' + col_list_tmp)
    col_list_len = len(col_list_tmp.split(','))

    col_info = []

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
            info = {"col": col, "comment": "自定义", "dtype":"string","id": i}
        # col_info[str(i)] = col_info_value

        col_info.append(info)
    return col_info


sql_col_dict = {
    "dayno": {"col": "fdate", "comment": "日期", "dtype": "string"},
    "fdate": {"col": "fdate", "comment": "日期", "dtype": "string"},
    "imei": {"col": "imei", "comment": "imei", "dtype": "string"},
    "evt_nums": {"col": "evt_nums", "comment": "应扣下载量", "dtype": "bigint"},
    "acc_nums": {"col": "acc_nums", "comment": "实扣下载量", "dtype": "bigint"},
    "acc_cost": {"col": "acc_cost", "comment": "实扣金额", "dtype": "bigint"},
    "expose_nums":{"col": "expose_nums", "comment": "曝光量", "dtype": "bigint"}
}

print(get_sql_col_info(sql))

sql_col_info = get_sql_col_info(sql)
print(len(sql_col_info))

hive_text = ''
mysql_text = ''
oflow_text = ''
for i in range(len(sql_col_info)):
    col_info = sql_col_info[i]
    mysql_text += '{col} {dtype} comment "{comment}",\n'.format(**col_info)
    if i == len(sql_col_info) - 1:
        hive_text += '{col} {dtype} comment "{comment}"\n'.format(**col_info)
        oflow_text += '{col}'.format(**col_info)
    else:
        hive_text += '{col} {dtype} comment "{comment}",\n'.format(**col_info)
        oflow_text += '{col},'.format(**col_info)

hive_head = 'create table ' + db_name + '.'+ tbl_name + '\n('
hive_foot = ')' + ' comment "' + tbl_comment + '"\nPARTITIONED BY (dayno bigint COMMENT "日期")\nSTORED AS ORCFILE;'

mysql_head = "create table " + tbl_name + '\n(\n' + 'id bigint unsigned NOT NULL AUTO_INCREMENT COMMENT "主键ID",\ndt date NOT NULL DEFAULT "0000-00-00" COMMENT "日期,时间格式日期",\n'
mysql_foot = 'PRIMARY KEY (`id`,`dt`),\nKEY `idx_dt` (`dt`)\n) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT="' + tbl_comment + '"\nPARTITION BY RANGE (TO_DAYS(dt))\n(PARTITION pmin VALUES LESS THAN (737850) ENGINE = InnoDB,\nPARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB);'

oflow_head = "sh /data1/etl_sys/script/tools_mysql_export_bi.sh {{ execution_date.strftime ('%Y%m%d') }} 'null id,fdate dt,"
oflow_foot =  "' '" + db_name + '.' + tbl_name + "' 0 1=1 bip0 " + tbl_name



hive_query = hive_head + '\n' + hive_text + hive_foot
sql_query = mysql_head + mysql_text + mysql_foot
oflow_query = oflow_head + oflow_text + oflow_foot
print('## Hive 建表语句\n')
print(hive_query)
print('\n---------------------------------\n')

print('## MySQL 建表语句\n')
print(sql_query)
print('---------------------------------')

print('## oflow MySQL 推数语句\n')
print(oflow_query)

# %%
# create_table_query="""
# # fdate\tstring\t日期
# # ocpctype\tstring\toCPX类型
# # expose_nums\tbigint\t曝光量
# # evt_nums\tbigint\t下载量
# # acc_cost\tdecimal(10,6)\t实扣金额
# # """

# print(create_table_query)

# # %%

# # 逐个字符遍历
# # n = 0
# # for line in create_table_query:
# #     print(line, n)
# #     n += 1

# # 通过字典设置参数
# # site = {"name": "菜鸟教程", "url": "www.runoob.com"}
# # print("网站名：{name}, 地址 {url}".format(**site))


# # Hive 建表语句
# hive_head = 'create table ' + db_name + '.'+ tbl_name + '\n('

# lines = re.split('\n', create_table_query)
# # print(len(lines))
# hive_text = ''
# for idx, line in enumerate(lines):
#     if line != '':
#         query = {"col": line.split('\t')[0], "type": line.split('\t')[1], "comment": line.split('\t')[2]}
#         if idx == len(lines) - 2:
#             hive_text += '{col} {type} comment "{comment}"\n'.format(**query)
#         else:
#             hive_text += '{col} {type} comment "{comment}",\n'.format(**query)



# hive_foot = ')' + ' comment "' + tbl_comment + '"\nPARTITIONED BY (dayno bigint COMMENT "日期")\nSTORED AS ORCFILE;'
# # print(sql_head, sql_foot)

# hive_query = hive_head + '\n' + hive_text + hive_foot
# print('## Hive 建表语句\n')
# print(hive_query)
# print('\n---------------------------------\n')

# # MySQL 建表语句

# mysql_head = "create table " + tbl_name + '\n(\n' + 'id bigint unsigned NOT NULL AUTO_INCREMENT COMMENT "主键ID",\ndt date NOT NULL DEFAULT "0000-00-00" COMMENT "日期,时间格式日期",\n'

# mysql_text = ""
# for idx, line in enumerate(lines):
#     if line != '':
#         if line.split('\t')[1].lower() == 'string':
#             query = {"col": line.split('\t')[0], "type": "varchar(40)", "comment": line.split('\t')[2]}
#         else:
#             query = {"col": line.split('\t')[0], "type": line.split('\t')[1], "comment": line.split('\t')[2]}
#         mysql_text += '{col} {type} comment "{comment}",\n'.format(**query)

# mysql_foot = 'PRIMARY KEY (`id`,`dt`),\nKEY `idx_dt` (`dt`)\n) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT="' + tbl_comment + '"\nPARTITION BY RANGE (TO_DAYS(dt))\n(PARTITION pmin VALUES LESS THAN (737850) ENGINE = InnoDB,\nPARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB);'

# sql_query = mysql_head + mysql_text + mysql_foot

# print('## MySQL 建表语句\n')
# print(sql_query)
# print('---------------------------------')

# oflow_head = "sh /data1/etl_sys/script/tools_mysql_export_bi.sh {{ execution_date.strftime ('%Y%m%d') }} 'null id,fdate dt, fdate,"

# oflow_text = ''
# for idx, line in enumerate(lines):
#     if line != '':
#         query = {"col": line.split('\t')[0], "type": line.split('\t')[1], "comment": line.split('\t')[2]}
#         if idx == len(lines) - 2:
#             oflow_text += '{col}'.format(**query)
#         else:
#             oflow_text += '{col}, '.format(**query)

# oflow_foot =  "' '" + db_name + '.' + tbl_name + "' 0 1=1 bip0 " + tbl_name


# oflow_query = oflow_head + oflow_text + oflow_foot

# print('## oflow MySQL 推数语句\n')
# print(oflow_query)