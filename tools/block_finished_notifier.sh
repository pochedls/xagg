#!/bin/bash

# bash script will print off a notification to the command line whenever the current log file
# changes (implying that a block of files has finished). 
#
# https://superuser.com/questions/181517/how-to-execute-a-command-whenever-a-file-changes

### Get file
fn=$(ls ../*.log | sort -V | tail -n 1)

### Set initial time of file
LTIME=`stat -c %Z $fn`

date

while true    
do
   ATIME=`stat -c %Z $fn`

   if [[ "$ATIME" != "$LTIME" ]]
   then
       echo -e "\e[31m##############\e[0m"    
       echo -e "\e[31m##############\e[0m"    
       echo -e "\e[31m##############\e[0m"    
       echo -e "\e[31mBlock Finished\e[0m"
       echo -e "\e[31m##############\e[0m"    
       echo -e "\e[31m##############\e[0m"    
       echo -e "\e[31m##############\e[0m"    
       date
       LTIME=$ATIME
   fi
   sleep 30
done