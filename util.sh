#!/bin/bash

# Copy src to the cluster $1 = itu_username and $2 = path_to_folder
function src2cluster() {
  zip -r src.zip src;
  scp -P 8022 src.zip $1@130.226.142.166:~/$2;	
}

# Ssh into ambari cluster $1 = itu_username
function ssh2ambari() {  
  ssh $1@130.226.142.166 -p 8022; 
}

