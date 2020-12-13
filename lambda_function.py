import json
import boto3
s3 = boto3.client('s3')

def get_books(query): #шаблон запроса к Google Books API
    import urllib3, json
    http=urllib3.PoolManager()
    url = 'https://www.googleapis.com/books/v1/volumes?q=%s&country=RU' % (query)
    r = http.request('GET',url)
    return (r.data) 

def create_db(data): #создание базы данных
	import pandas as pd
	df1=pd.DataFrame(columns=['title','description','google books link'])
	books=json.loads(data.decode('utf8'))['items']
	for i in range(0,len(books)):
		if 'description' in books[i]['volumeInfo']:
			book=books[i]['volumeInfo']
			df.loc[i]=[book['title'],book['description'],book['infoLink']]
			df1=df.reset_index(drop = True)
	return(df1)

def lambda_handler(event, context): #считывание файла S3, создание запроса к Google Books API, загрузка нужных данных о книгах в базу данных
	from urllib.parse import unquote_plus
	for record in event['Records']:
		key = unquote_plus(record['s3']['object']['key'])
		bucket_name = record['s3']['bucket']['name']
		file_key = record['s3']['object']['key']
		obj = s3.get_object(Bucket=bucket_name, Key=file_key)
		fnl = obj['Body'].read().split(b'\n')
		query = json.loads(fnl[0].decode("utf-8"))['genre']
		booklist = get_books(query)
	return(create_db(booklist))