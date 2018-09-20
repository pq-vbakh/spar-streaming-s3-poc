1. Set AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY env variables at your IDEA run configurations. 
   Ensure tmp3049/data/vector file exists on application startup 
2. nc -lk 9999 # to trigger any console output you have to write some data to socket each time 
3. Start SpeedLayer (multiply stream by S3 vector) # multiple input stream from socket (shell) by S3 vector
4. Start BatchLayer (update S3 vector from time to time)