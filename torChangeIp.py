import requests
from stem import Signal
from stem.control import Controller
import socket
import socks
import timeout_decorator

proxies = {
    'http': '127.0.0.1:8118'
}

class torChangeIp:
    @timeout_decorator.timeout(5)
    def __init__(self):
        self.initial_ip = (requests.get('http://ident.me', proxies=proxies).text)
        with Controller.from_port(port = 9051) as c:
            c.authenticate()
            c.signal(Signal.NEWNYM)

    def newIp(self):
        response = (requests.get('http://ident.me', proxies=proxies).text)
        return response != self.initial_ip



