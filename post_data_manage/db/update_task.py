import pymysql

# 打开数据库连接
db = pymysql.connect(host='211.71.76.189',
                     user='root',
                     password='669988',
                     database='postman')

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# 使用 execute()  方法执行 SQL 查询
cursor.execute("SELECT VERSION()")

# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()

print("Database version : %s " % data)


def update_task(uuid, status):
    # 使用 execute()  方法执行 SQL 查询
    sql = "update post_task set post_task_status = %s where post_task_id = '%s'" % (
        status, uuid)
    print(sql)
    cursor.execute(sql)
    db.commit()


update_task('uuid', 2)
