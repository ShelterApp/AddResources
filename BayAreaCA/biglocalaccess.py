import requests as req
import os

token = os.environ['BIG_LOCAL_NEWS_BAY_AREA_COVID_API_KEY']
endpoint = 'https://api.biglocalnews.org/graphql'
token_type = 'JWT'

def gql(query, vars={}):
    res = req.post(
        endpoint,
        json={'query':query, 'variables':vars},
        headers={'Authorization':f'{token_type} {token}'}
    )
    res.raise_for_status() #raises err if not HTTP resp:200(OK)
    return res.json()
def download(uri, dest_file_path):
    with open(dest_file_path, 'wb') as file:
        headers = {'content-type': 'application/octet-stream',
                   'host':'storage.googleapis.com'}
        res = req.get(uri, headers = headers)
        res.raise_for_status()
        file.write(res.content)


user = '''
query {
  user {
    name    
  }
}
'''
downloadFile='''
mutation DownloadFile(
    $input: FileURIInput!
    ){
    createFileDownloadUri


res = gql(user)
print(res)
