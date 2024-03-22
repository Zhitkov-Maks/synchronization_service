import json
from datetime import datetime as dt
from typing import Dict

import requests

from cloud_exceptions import TokenException, CloudException


class YandexCloud:
    """
    Класс для работы с яндекс облаком.

    Args:
        token (str): Передается токен для аутентификации с яндекс диском.
        name_folder_cloud (str): Передается в облаке с которой будет синхронизация.

    Attributes:
        url (str): Базовый url для запросов к диску.
    """
    url = "https://cloud-api.yandex.net/v1/disk/resources"

    def __init__(self, token: str, name_folder_cloud: str):
        self.name_folder_cloud = name_folder_cloud
        self.headers = {
            "Content-type": "application/json",
            "Accept": 'application/json',
            "Authorization": f"OAuth {token}"
        }

    def __save(self, url: str, path: str, file_name: str) -> None:
        """
        Метод для сохранения файла в облаке, нужен для методов load и reload,
        так как методы практически одинаковые.

        :param url: Сформированный url для загрузки файла.
        :param path: Путь к файлу на пк.
        :param file_name: Имя файла который нужно сохранить.
        :raise CloudException: Если запрос завершился кодом отличным от 200, пробрасываем исключение.
        """
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data: json = response.json()
            with open(f"{path}/{file_name}", "rb") as file:
                requests.put(data['href'], files={'file': file})
        else:
            message: str = f"Файл: {file_name}, Ошибка: {response.json().get('message')}"
            raise CloudException(message)

    def load(self, path: str, file_name: str) -> None:
        """
        Метод формирует url для загрузки файла., и отправляет непосредственно на сохранение.
        :param path: Путь к файлу.
        :param file_name: Имя файла для сохранения в облаке.
        """
        url: str = f"{self.url}/upload?path={self.name_folder_cloud}/{file_name}&overwrite=False"
        self.__save(url, path, file_name)

    def reload(self, path: str, file_name: str) -> None:
        """
        Метод формирует url для перезаписи файла., и отправляет непосредственно на сохранение.

        :param path: Путь к файлу.
        :param file_name: Имя файла для сохранения в облаке.
        """
        url = f"{self.url}/upload?path={self.name_folder_cloud}/{file_name}&overwrite=True"
        self.__save(url, path, file_name)

    def delete(self, filename: str) -> None:
        """
        Метод для удаления файла в облаке.

        :param filename: Имя удаляемого файла.
        :raise CloudException: Если запрос завершился кодом отличным от 204, пробрасываем исключение.
        """
        url: str = f"{self.url}?path={self.name_folder_cloud}/{filename}&force_async=False&permanently=False"
        response = requests.delete(url, headers=self.headers)
        if response.status_code != 204:
            message: str = f"Файл: {filename}, Ошибка: {response.json().get('message')}"
            raise CloudException(message)

    def get_info(self) -> Dict[str, float] | None:
        """
        Метод для получения списка файлов в облачной папке.

        :return dict: Возвращает словарь, где ключ имя файла, значение последнее изменение файла.
        :raises CloudException, TokenException: Если запрос завершился кодом 401, это значит
            что проблемы с токеном, если какие-то другие проблемы, то вызываем CloudException.
        """
        url: str = f"{self.url}?path={self.name_folder_cloud}&fields=items&limit=10000"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            files: dict = {}
            for item in response.json()["_embedded"]["items"]:
                files[item.get("name")] = dt.fromisoformat(item.get("modified")).timestamp()
            return files

        elif response.status_code == 401:
            raise TokenException
        else:
            raise CloudException(response.json().get("message"))

    def __create_folder_cloud(self):
        """
        Метод для создания папки в облаке.

        :raise CloudException: Если произошел непредвиденный сбой.
        """
        url = f"{self.url}?path={self.name_folder_cloud}"
        response = requests.put(url, headers=self.headers)
        if response.status_code != 201:
            message: str = f"Ошибка: {response.json().get('message')}"
            raise CloudException(message)

    def check_exists_folder_cloud(self):
        """
        Метод для проверки существует ли папка указанная в dotenv на яндекс диске,
        если нет, то отправляем на создание таковой.
        """
        url: str = f"{self.url}?path={self.name_folder_cloud}&limit=1"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 404:
            self.__create_folder_cloud()
