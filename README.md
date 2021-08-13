To achieve parallel execution in airflow,
1.As SQLite doesn't support parallel execution,we have to move to MYSQl connection 
2.install MySQl in the computer and connect to server.
3.To install and connection setup follow these link
https://www.digitalocean.com/community/tutorials/how-to-install-mysql-on-ubuntu-20-04
4.for parallel execution change sequential execution to executor = LocalExecutor in the airflow config file 
5.create a fernet key and attach that in airflow config file (for that code is attached)
6.after that,to schedule the execution of airflow, scheduling is done 

