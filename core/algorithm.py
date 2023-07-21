from abc import ABC, abstractmethod


class Algorithm(ABC):
  @abstractmethod
  def __call__(self, cluster, clock, backfill, allocation_func):
    pass