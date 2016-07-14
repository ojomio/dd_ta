### Для запуска:

* `cd package`
* `tar -xjf db.bzip2` для распаковки скачанной базы
* `pip install -r requirements.txt` для установки зависимостей
* `PYTHONPATH=.. ./parse.py` для парсинга или `PYTHONPATH=.. jupyter notebook` для просмотра ноутбука

#### Не забудьте для ноутбука
* `jupyter nbextension enable --py --sys-prefix widgetsnbextension`
* `jupyter nbextension enable --py --user gmaps`


