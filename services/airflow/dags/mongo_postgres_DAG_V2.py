from datetime import timedelta

import airflow
import numpy as np
import pandas as pd
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient, ReplaceOne
from sqlalchemy import create_engine
from datetime import datetime as dt
import datetime
from airflow.models import Variable

MONGO_DATABASE = 'dh'
MONGO_COLLECTION = ['questions', 'answers']
POSTGRES_TABLE = 'mongo_data'


def get_conn(uri):
    """
    Fetches PyMongo Client
    """
    if uri is None:
        return "uri not present"
    else:
        client = MongoClient(uri)
    return client


def get_mongo_data(mongo_collection, **kwargs):
    """Gets query output in dataframe
    :param str query:
        SQL query string
    :param str cursor
        Cursor object
    :return:
        A SQL result in dataframe
    :rtype: object
    """
    if not mongo_collection:
        raise ValueError("collection is None or empty.")
    database = get_conn(Variable.get("MONGO_URI"))[MONGO_DATABASE]
    collection = database[mongo_collection]
    df = pd.DataFrame(list(collection.find()))
    # df.to_csv(TEMP_FILE.format(mongo_collection), index=False)
    return df


def data_transform(**kwargs):
    def get_answer(ques, timestamp):
        record = merged_df[
            (merged_df["_id_x"] == ques) &
            (merged_df["timestamp"] == timestamp)
        ]
        if len(record) > 0:
            return record.iloc[0]["Answer"]
        return None

    def get_answer_food(ques, timestamp):
        try:
            record = merged_df[
                (merged_df["_id_x"] == ques) &
                (merged_df["timestamp"] == timestamp)
            ]
            answers = ""
            if len(record) > 0:
                for index, row in record.iterrows():
                    answers = answers + str(row["Answer"].encode('utf-8').strip().decode()) + ","
                return answers.strip(",")
            return None
        except Exception as e:
            print(e) 
            print (timestamp)
            return None
    ti = kwargs['ti']
    questions_df = ti.xcom_pull(task_ids='extract_mongo_questions')
    answers_df = ti.xcom_pull(task_ids='extract_mongo_answers')
    questions_df["_id"] = questions_df["_id"].map(str)
    answers_df["question"] = answers_df["question"].map(str)
    questions_df = questions_df[["_id","body","timeOption"]]
    # Now merge questiondf with answerdf
    merged_df = pd.merge(questions_df, answers_df, left_on="_id", right_on="question")
    # rename & slice columns
    merged_df.rename(index=str,columns={"body_x": "Question", "body_y":"Answer","_id_y":"_id"}, inplace=True)
    merged_df["timestamp"] = merged_df["updatedAt"].dt.date
    merged_df["timestamp"] = pd.to_datetime(merged_df["timestamp"])
    # max_date = merged_df.updatedAt.max()
    max_date = merged_df.updatedAt.max()
    initial_day = dt(2021,4,2)
    add_delta = (max_date - initial_day).days
    days = pd.date_range(initial_day, initial_day + timedelta(add_delta), freq='D')
    df = pd.DataFrame({'timestamp': days})
    df["latenightsnake"] = df[["timestamp"]].apply(lambda x:get_answer("606759aed9c559001ce9b6cf", x[0]), axis=1)
    df["weight"] = df[["timestamp"]].apply(lambda x:get_answer("60667c6380a467001c362a88", x[0]), axis=1)
    df["weight"] = df["weight"].fillna(0)
    def remove_spaces(x):
        if " " in str(x):
            return str(x)[-4:]
        else:
            return x
    df["weight"] = df["weight"].apply(lambda x : remove_spaces(x))
    df["weight"] = df["weight"].astype('float')
    # Morning
    df["breakfast"] = df[["timestamp"]].apply(lambda x:get_answer("60667c7380a467001c362a89", x[0]), axis=1)
    df["beverages"] = df[["timestamp"]].apply(lambda x:get_answer("60667c7c80a467001c362a8a", x[0]), axis=1)
    df["bathroom"] = df[["timestamp"]].apply(lambda x:get_answer("60667c8780a467001c362a8b", x[0]), axis=1)
    # Noon
    df["food"] = df[["timestamp"]].apply(lambda x:get_answer_food("60667c9580a467001c362a8c", x[0]), axis=1)
    df["sweet"] = df[["timestamp"]].apply(lambda x:get_answer("60667cc380a467001c362a8f", x[0]), axis=1)
    # Night
    df["nightfood"] = df[["timestamp"]].apply(lambda x:get_answer("60667cd680a467001c362a90", x[0]), axis=1)
    df["night_beverages"] = df[["timestamp"]].apply(lambda x:get_answer("60667ce880a467001c362a91", x[0]), axis=1)
    df["exercise"] = df[["timestamp"]].apply(lambda x:get_answer("60667cf380a467001c362a92", x[0]), axis=1)
    df["calories"] = df[["timestamp"]].apply(lambda x:get_answer("60667cfc80a467001c362a93", x[0]), axis=1)
    df["emotions"] = df[["timestamp"]].apply(lambda x:get_answer("60667d0480a467001c362a94", x[0]), axis=1)
    # food
    df["drink_or_food"] = df[["timestamp"]].apply(lambda x:get_answer_food("60667d1480a467001c362a95", x[0]), axis=1)
    df["reflux"] = df[["timestamp"]].apply(lambda x:get_answer("6071e7c5c3a95a5a31824d03", x[0]), axis=1)
    # top_cols = ['WakingUp', 'WakingUp',"Morning","Morning","Morning","afternoon","afternoon","Night","Night","Night","Night","Night","food","food"]
    # o_cols = ['latenightsnake', 'weight',"breakfast","beverages","bathroom","food","sweet","nightfood","night_beverages","exercise","calories","emotions","Drink or food" ,"reflux"]
    # df = df.set_index("timestamp")
    # df.columns = [top_cols, o_cols]
    return df


def insert_postgres_data(**kwargs):
    """Inserts dataframe in postgres
    :return:
        insert rows
    :rtype: object
    """
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='data_transform')
    engine = create_engine(Variable.get("POSTGRES_CONN_STRING"))
    df.to_sql(POSTGRES_TABLE, engine, index=False, if_exists='replace')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'airflow_dag_mongo_postgres',
    default_args=default_args,
    schedule_interval='0 11 * * *',
)


start_task = DummyOperator(
    task_id='start',
    dag=dag
)


end_task = DummyOperator(
    task_id='end',
    dag=dag
)

extract_mongo_questions = PythonOperator(
    task_id='extract_mongo_questions',
    provide_context=True,
    python_callable=get_mongo_data,
    op_kwargs={'mongo_collection': MONGO_COLLECTION[0]},
    dag=dag)

extract_mongo_answers = PythonOperator(
    task_id='extract_mongo_answers',
    provide_context=True,
    python_callable=get_mongo_data,
    op_kwargs={'mongo_collection': MONGO_COLLECTION[1]},
    dag=dag)

data_transform = PythonOperator(
    task_id='data_transform',
    provide_context=True,
    python_callable=data_transform,
    dag=dag)
    
insert_postgres = PythonOperator(
    task_id='insert_postgres',
    provide_context=True,
    python_callable=insert_postgres_data,
    dag=dag)


start_task >> extract_mongo_questions
start_task >> extract_mongo_answers
extract_mongo_questions >> data_transform
extract_mongo_answers >> data_transform
data_transform >> insert_postgres >> end_task