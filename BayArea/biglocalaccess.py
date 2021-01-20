import requests as req
from config import *

token = key
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

user = '''
query {
  user {
    name    
  }
}
'''

res = gql(user)
print(res)