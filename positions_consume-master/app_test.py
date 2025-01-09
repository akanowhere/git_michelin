#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""
Created on Wed Jan 29 13:54:28 2020
@author: igor.seibel.ext
"""
import app
import unittest
import threading
from time import sleep

def call_script():
     while True:
         sleep(5)
         return True

class AppTest(unittest.TestCase):

    def setUp(self):
        app.app.testing = True
        self.test_app = app.app.test_client()

    def test_home(self):
        result = self.test_app.get('/')
        assert(result, "Welcome!")
 
    def test_health(self):
        th = threading.Thread( target=call_script, name="Thread-tpms")
        th.start()        
        result = self.test_app.get('/actuator/health')
        assert(result, "Health")

    def test_prometheus(self):
        result = self.test_app.get('/actuator/prometheus')
        self.assertEqual(result.status_code, 302)