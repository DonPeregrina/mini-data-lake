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


MONGO_HOST = '127.0.0.1'
MONGO_PORT = 27017
MONGO_URI = 'mongodb+srv://orquestratorDH:exile.2021@main.yibma.mongodb.net/dh?retryWrites=true&w=majority'
MONGO_DATABASE = 'dh'
MONGO_COLLECTION = ['questions', 'answers']
MONGO_BATCHSIZE = 1000
POSTGRES_CONN_STRING = 'postgresql+psycopg2://staging_usr:cyber2022@143.244.222.148:5432/staging'
POSTGRES_TABLE = 'mongo_data'
TEMP_FILE = '/home/azureuser/airflow/dags/extract_mongo_{}.csv'



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
    database = get_conn(MONGO_URI)[MONGO_DATABASE]
    collection = database[mongo_collection]
    df = pd.DataFrame(list(collection.find()))
    # df.to_csv(TEMP_FILE.format(mongo_collection), index=False)
    return df


def data_transform(**kwargs):
    def get_answer(ques, timestamp):
        record = merged_df[
            (merged_df["_id_x"] == ques) &
            (merged_df["TimeStamp"] == timestamp)
        ]
        if len(record) > 0:
            return record.iloc[0]["Answer"]
        return None

    def get_answer_food(ques, timestamp):
        try:
            record = merged_df[
                (merged_df["_id_x"] == ques) &
                (merged_df["TimeStamp"] == timestamp)
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
    merged_df["TimeStamp"] = merged_df["updatedAt"].dt.date
    merged_df["TimeStamp"] = pd.to_datetime(merged_df["TimeStamp"])
    # max_date = merged_df.updatedAt.max()
    max_date = merged_df.updatedAt.max()
    initial_day = dt(2021,4,2)
    add_delta = (max_date - initial_day).days
    days = pd.date_range(initial_day, initial_day + timedelta(add_delta), freq='D')
    df = pd.DataFrame({'TimeStamp': days})
    df["LateNightSnake"] = df[["TimeStamp"]].apply(lambda x:get_answer("606759aed9c559001ce9b6cf", x[0]), axis=1)
    df["Weight"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667c6380a467001c362a88", x[0]), axis=1)
    # Morning
    df["BreakFast"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667c7380a467001c362a89", x[0]), axis=1)
    df["Beverages"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667c7c80a467001c362a8a", x[0]), axis=1)
    df["Bathroom"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667c8780a467001c362a8b", x[0]), axis=1)
    # Noon
    df["Food"] = df[["TimeStamp"]].apply(lambda x:get_answer_food("60667c9580a467001c362a8c", x[0]), axis=1)
    df["Sweet"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667cc380a467001c362a8f", x[0]), axis=1)
    # Night
    df["NightFood"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667cd680a467001c362a90", x[0]), axis=1)
    df["Night_Beverages"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667ce880a467001c362a91", x[0]), axis=1)
    df["Exercise"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667cf380a467001c362a92", x[0]), axis=1)
    df["Calories"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667cfc80a467001c362a93", x[0]), axis=1)
    df["Emotions"] = df[["TimeStamp"]].apply(lambda x:get_answer("60667d0480a467001c362a94", x[0]), axis=1)
    # Food
    df["Drink or Food"] = df[["TimeStamp"]].apply(lambda x:get_answer_food("60667d1480a467001c362a95", x[0]), axis=1)
    df["Reflux"] = df[["TimeStamp"]].apply(lambda x:get_answer("6071e7c5c3a95a5a31824d03", x[0]), axis=1)
    # top_cols = ['WakingUp', 'WakingUp',"Morning","Morning","Morning","afternoon","afternoon","Night","Night","Night","Night","Night","Food","Food"]
    # o_cols = ['LateNightSnake', 'Weight',"BreakFast","Beverages","Bathroom","Food","Sweet","NightFood","Night_Beverages","Exercise","Calories","Emotions","Drink or Food" ,"Reflux"]
    # df = df.set_index("TimeStamp")
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
    engine = create_engine(POSTGRES_CONN_STRING)
    df.to_sql(POSTGRES_TABLE, engine, index=False)


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
    schedule_interval=None,
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