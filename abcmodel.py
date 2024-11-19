from abc import ABC, abstractmethod


class ABCModel(ABC):

    @abstractmethod
    def fetch_page(self):
        pass

    @abstractmethod
    def process_page(self):
        pass




