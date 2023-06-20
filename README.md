## Dags_Airflow
r-shamjan.py
Dags that consider the following:
- Find top 10 domain zones by number of domains
- Find the domain with the longest name (if there are several, then take only the first one in alphabetical order)
- Where is the airflow.com domain located?
The final task writes to the log the result of the answers to the questions above

r-shamjan_l3.py
DAG of several tasks, as a result of which it finds answers to the following questions:
- What game was the best-selling this year worldwide?
- What genre of games were the best-selling in Europe? List all if there is more than one
- Which platform had the most games that sold over a million copies in North America?
- List all if there is more than one
- Which publisher has the highest average sales in Japan?
- List all if there is more than one
- How many games have sold better in Europe than in Japan?

The final task writes the answer to each question to the log.
