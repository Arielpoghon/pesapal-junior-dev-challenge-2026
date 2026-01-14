## Demo Screenshot
![Students Manager] [<img width="1828" height="645" alt="image" src="https://github.com/user-attachments/assets/f6265bcc-3848-4a9a-9ef6-8f04ace197ad" />
]
## How to Run

### Prerequisites
- Python 3.8 or higher (tested on 3.14)
- Git (to clone the repo)

### 1. Clone the Repository
bash
git clone https://github.com/arielpoghon/pesapal-junior-dev-challenge-2026.git
cd pesapal-junior-dev-challenge-2026

## Create virtual environment
python -m venv venv

# Activate it
# Linux / macOS / Garuda:
source venv/bin/activate

# Windows:
# venv\Scripts\activate

## Install Flask
pip install -r requirements.txt

## Run the Interactive REPL
python repl.py

## At the db> prompt, try these example commands

CREATE TABLE students (id INTEGER, name TEXT, age INTEGER);
INSERT INTO students (name, age) VALUES ('Alice', 20);
INSERT INTO students (name, age) VALUES ('Bob', 25);
SELECT * FROM students WHERE age > 18;
UPDATE students SET age=21 WHERE name='Alice';
DELETE FROM students WHERE name='Bob';

## Run the Web Demo
python web_app.py

SHOW TABLES;
SHOW SCHEMA students;
.exit
