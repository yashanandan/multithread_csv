import csv, sqlite3
import random
from queue import Queue
from threading import Thread

connection = sqlite3.connect("ratings_csv.db")
cursor = connection.cursor()
create_query = "CREATE TABLE IF NOT EXISTS ratings"
columns = ""
no_of_threads = 8
max_lines_per_thread = 125000
lines_in_file = 26000000


def en_queue(queue, thread_no, starting_line):
    print("Starting to read line from " + str(starting_line) + " for thread " + str(thread_no))
    ratings_reader = open('/home/yash/Desktop/ml-latest/ratings.csv')
    for i, line in enumerate(ratings_reader):
        if i in range(starting_line, starting_line + max_lines_per_thread):
            queue.put((line.rstrip().split(",")))
    ratings_reader.close()
    print("Finished for thread " + str(thread_no))

def de_queue(queue, cursor):
    while not queue.empty():
        # print(queue.get())
        cursor.execute("INSERT INTO ratings VALUES (?, ?, ?, ?)", queue.get())
        queue.task_done()

with open('/home/yash/Desktop/ml-latest/ratings.csv', "r") as ratings:
    ratings_reader = csv.DictReader(ratings)
    headers = ratings_reader.fieldnames
    columns = ", ".join(headers)
    create_query = create_query + "( " + columns + " );"
    cursor.execute(create_query)
    ratings.close()
    queue = Queue()
    start_lines = []

    for i in range(no_of_threads):
        start_line = random.randint(0, lines_in_file)
        if not start_lines:
            start_lines.append(start_line)
        if start_line not in start_lines or len(start_lines) == 1:
            for start in start_lines:
                if start_line not in range(start, start + max_lines_per_thread):
                    start_lines.append(start_line)
                    worker = Thread(target=en_queue, args=(queue, i, start_line))
                    worker.setDaemon(True)
                    worker.start()
    queue.join()
    print(queue.qsize())
    de_queue(queue, cursor)
    connection.commit()
    # cursor.execute("SELECT * FROM ratings;")
    # rows = cursor.fetchall()
    # for row in rows:
    #     print(row)
