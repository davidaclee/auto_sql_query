from flask import Flask,request, render_template, Response, jsonify
from flask_wtf import Form
from wtforms import StringField, TextAreaField
import os
import json
import re
app = Flask(__name__)
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['JSON_AS_ASCII'] = False
@app.route('/sql', methods = ['GET', 'POST'])
def auto_sql_query():
    # form = InputSqlForm(request.form)
    # sql = form.sql.data
    # sql_col_info = get_sql_col_info(sql)
    # print(sql)

    return render_template('auto_sql_query.html')

class InputSqlForm(Form):
    sql = TextAreaField('Input SQL Query')

class OutputSqlForm(Form):
    id = StringField('id')
    col = StringField('col')
    dtype = StringField('dtype')
    comment = StringField('comment')

@app.route('/sql_analysis', methods = ['POST'])
def get_sql_col_info():
    query = request.get_json()
    print('Query post:', query)
    sql = query['sql']
    db_name = 'ad_da'
    tbl_name = query['tbl_name']
    tbl_comment = query['tbl_comment']
    sql = sql.lower()
    col_list_tmp = sql.replace('\n', '').replace('select', '').replace('from', '?').replace('where', '?')
    col_list_tmp = col_list_tmp.split('?')[0]

    # 替换调用函数时括号里的逗号
    p = re.compile('\,.*?\(.*?\)')
    col_list_tmp = p.sub(',', col_list_tmp)

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
            info = {"col": col, "comment": "自定义", "dtype":"string","id": i}
        # col_info[str(i)] = col_info_value

        sql_col_info.append(info)
    # print(json.dumps(col_info))
    print('Analysis Result:', sql_col_info)
    print('Jsonify Result:', jsonify(sql_col_info))

    # return Response(

    # sql_col_info = get_sql_col_info(sql)
    # print(len(sql_col_info))

    hive_text = ''
    mysql_text = ''
    oflow_text = ''
    for i in range(len(sql_col_info)):
        col_info = sql_col_info[i]
        if col_info['dtype'] == 'string':
            mysql_text += '{col} varchar(40) comment "{comment}",\n'.format(**col_info)
        else:
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
    mysql_query = mysql_head + mysql_text + mysql_foot
    oflow_query = oflow_head + oflow_text + oflow_foot

    all_query = '<b># Hive 建表语句</b>' + '\n' + hive_query + '\n\n' + '<b># MySQL 建表语句</b>' + '\n' + mysql_query + '\n\n' + '<b># oflow 执行语句</b>' + '\n' + oflow_query

    tbl_query = {
        'all_query': all_query
    }

    # print(tbl_query)

    return jsonify(tbl_query)
    # return jsonify({'ok': True})


sql_col_dict = {
    "dayno": {"col": "dayno", "comment": "日期", "dtype": "string"},
    "imei": {"col": "imei", "comment": "imei", "dtype": "string"},
    "evt_nums": {"col": "evt_nums", "comment": "应扣下载量", "dtype": "bigint"},
    "acc_nums": {"col": "acc_nums", "comment": "实扣下载量", "dtype": "bigint"},
    "acc_cost": {"col": "acc_cost", "comment": "实扣金额", "dtype": "bigint"},
    "expose_nums":{"col": "expose_nums", "comment": "曝光量", "dtype": "bigint"}
}

if __name__ == "__main__":
    app.run()


# select
# dayno
# ,app_id
# ,count(1) down_nums
# from ad.f_ads_cpd_srh_down
# where dayno = 20200923
# group by dayno, app_id