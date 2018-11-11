#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import tweepy


# In[ ]:


from tweepy import OAuthHandler, Stream


# In[ ]:


from tweepy.streaming import StreamListener


# In[ ]:


import socket
import json


# In[ ]:


consumer_key = 'PARwB8C5sjwr0B4y8hWYlwiFZ'
consumer_secret = 'WGqhDGdX8sNycTyRqkggYFWtbyQHx6o2LuQsflHSIcj41UTVUW'
access_token = '29685243-N8E3HqkbrMoMY4HJYp89Y2c3lvHROqwzPibtx2yBm'
access_secret = 'iS8dBZ03jsghuQ9BuYbHmQ3iTfqgeCdF5vPo5qYC1lBLS'


# In[ ]:


class TweetListener(StreamListener):
    
    def __init__(self,csocket):
        self.client_socket = csocket
        
    def on_data(self,data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error ",e)
        return True
    
    def on_error(self,status):
        print(status)
        return True
        


# In[ ]:


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track = ['highered'])


# In[ ]:


if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 5558
    s.bind((host,port))
    
    print('listening on port 5558')
    
    s.listen (5)
    c,addr = s.accept()
    sendData(c)


# In[ ]:




