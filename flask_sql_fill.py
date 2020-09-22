from flask import Flask,request, render_template
from flask_wtf import Form
from wtforms import StringField, TextAreaField
import os
app = Flask(__name__)
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

@app.route('/sql', methods = ['GET', 'POST'])
def auto_sql_query():
    form = InputSqlForm(request.form)
    sql = form.sql.data
    # sql_col_info = get_sql_col_info(sql)

    return render_template('auto_sql_query.html')

class InputSqlForm(Form):
    sql = TextAreaField('Input SQL Query')

class OutputSqlForm(Form):
    id = StringField('id')
    col = StringField('col')
    dtype = StringField('dtype')
    comment = StringField('comment')

@app.route('/sql_analysis', methods = ['POST'])
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
    "dayno": {"col": "dayno", "comment": "日期", "dtype": "string"},
    "imei": {"col": "imei", "comment": "imei", "dtype": "string"},
    "evt_nums": {"col": "evt_nums", "comment": "应扣下载量", "dtype": "bigint"},
    "acc_nums": {"col": "acc_nums", "comment": "实扣下载量", "dtype": "bigint"},
    "acc_cost": {"col": "acc_cost", "comment": "实扣金额", "dtype": "bigint"},
    "expose_nums":{"col": "expose_nums", "comment": "曝光量", "dtype": "bigint"}
}

if __name__ == "__main__":
    app.run()