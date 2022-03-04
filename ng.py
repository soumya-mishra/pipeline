# -*- coding: utf-8 -*-
"""
Created on Fri Mar  4 08:30:22 2022

@author: 7000029183
"""

from pyngrok import ngrok
ngrok.kill()
# ngrok.kill()
myurl = ngrok.connect(port=8050).public_url
print(myurl)
# ngrok.kill()