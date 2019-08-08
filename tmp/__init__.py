import logging

from flask import Flask

formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s %(filename)s:%(lineno)d')


def init_logger():
    """"""
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    logger1 = logging.getLogger('logger1')
    logger1.setLevel(logging.DEBUG)
    logger1.addHandler(console_handler)

    # file_handler = logging.FileHandler('t.log')

    h = logger1.hasHandlers()
    print(h)
    return logger1


class A():

    def __init__(self):
        self.logger = init_logger()

    def run(self):
        self.logger.info('xxx')


def server():
    flask = Flask('a')

    @flask.route('/')
    def get():
        return '123'

    def post():
        with open('Django-2.2-py3-none-any.whl', 'rb') as r:
            r.read()
    flask.run()


if __name__ == '__main__':
    # for i in range(3):
    #     A().run()
    server()
