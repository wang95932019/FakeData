import pymysql
import yaml
from faker import Faker

# 读取配置文件
with open("config.yaml") as file:
    config = yaml.safe_load(file)  # 字典，保存配置信息

# 产生Faker对象，并本地化
fake = Faker("zh_CN")


def generate_data(values: list, num: int = config['num']) -> list:
    """
    生成假数据。
    values：需要生成的数据类型。['name','age']
    num：需要产生的数据条目
    @return：返回值格式[{'name': '傅桂花', 'score': 90, 'id': 'eb5914436c37aac3e37596d9e1b77762'},……]
    """

    # 传出的参数与方法之间的对应关系
    method_dict = config["method_dict"]

    # 将结果保存为列表，其中存放这字典
    data = []
    for _ in range(num):
        result = {}
        for value in values:
            result[value] = eval(method_dict[value])
        data.append(result)
    return data


def import_mysql(data: list, table: str = config["mysql"]['table']):
    # 数据库连接配置
    con_config = {k: v for k, v in config["mysql"].items() if k != 'table'}

    with pymysql.connect(**con_config) as db:
        cursor = db.cursor()
        for result in data:  # result是一个字典，写插入数据库的代码
            cols = ", ".join(f'`{k}`' for k in result.keys())
            val_cols = ', '.join(f'%({k})s' for k in result.keys())
            sql = f"insert into {table}(%s) values(%s)"
            res_sql = sql % (cols, val_cols)
            cursor.execute(res_sql, result)  # 将字典result传入

        db.commit()  # 统一提交


if __name__ == "__main__":
    values = ['name', 'score', 'id']
    data = generate_data(values)
    import_mysql(data)

    print(config)
