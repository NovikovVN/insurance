import sqlite3
from collections import namedtuple, deque
from datetime import datetime

from flask import Flask, render_template, redirect, url_for, request
from classifier import Classifier

app = Flask(__name__)

# Создание объекта классификатора - синтетической сигмоиды.
## Ответы: 0, 1, -1 (если не удается классифицировать введенное)
clf = Classifier()

# Ограничение отображаемых записей
maxlen = 10

# Загрузка последних записей из БД
with sqlite3.connect('texts.db') as conn:
    cursor = conn.cursor()
    cursor.execute('create table if not exists texts(now text, text_1 text, text_2 text, text_tag text)')
    cursor.execute('SELECT * FROM texts ORDER BY now DESC LIMIT {}'.format(maxlen))
    rows = cursor.fetchall()
    conn.commit()

# Создание записей для отображения
Text = namedtuple('Text', ['now', 'text_1', 'text_2', 'text_tag'])
all_texts = deque(maxlen=maxlen)
for row in rows:
    all_texts.append(Text(*row))


# Рендер страницы с передачей записей
@app.route('/', methods=['GET'])
def main():
    return render_template('add_text.html', all_texts=all_texts)


# Обработка POST-запроса
@app.route('/add_text', methods=['POST'])
def add_text():
    ## Получение данных
    now = datetime.now()
    text_1 = request.form['text_1']
    text_2 = request.form['text_2']
    text_tag = clf.predict([text_1, text_2])

    ## Добавление записи для отображения
    all_texts.appendleft(Text(now, text_1, text_2, text_tag))

    ## Добавление данных в БД
    with sqlite3.connect('texts.db') as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO texts VALUES('{}', '{}', '{}', '{}')".format( \
                        now, text_1, text_2, text_tag))
        conn.commit()

    ## Добавление записи в лог
    with open('log.log', 'a+', encoding='utf-8') as f:
        f.write('{}: {} {} TAG: {}\n'.format(now, text_1, text_2, text_tag))

    return redirect(url_for('main'))


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=False)