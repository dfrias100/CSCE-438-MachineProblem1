#!/bin/bash
ps -ef | grep coordinator | grep -v grep | awk '{print $2}' | xargs kill
ps -ef | grep tsd | grep -v grep | awk '{print $2}' | xargs kill
ps -ef | grep synchronizer | grep -v grep | awk '{print $2}' | xargs kill