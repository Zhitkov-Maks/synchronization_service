"""Модуль для создания своих исключений, для вызова их в классе с облаком."""


class CloudException(Exception):
    pass


class TokenException(Exception):
    pass
