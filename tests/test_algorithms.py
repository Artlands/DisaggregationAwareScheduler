from algorithms.rack_first import RackFirstAlgorithm

rf = RackFirstAlgorithm()
  
def test_sort_rack_id_1():
  rack_id = 3
  total_nracks = 6
  sorted_rack_id = rf.sort_rack_ids(rack_id, total_nracks)
  assert sorted_rack_id == [3, 2, 4, 1, 5, 0]

def test_sort_rack_id_2():
  rack_id = 2
  total_nracks = 6
  sorted_rack_id = rf.sort_rack_ids(rack_id, total_nracks)
  assert sorted_rack_id == [2, 1, 3, 0, 4, 5]


def test_sort_rack_id_3():
  rack_id = 0
  total_nracks = 6
  sorted_rack_id = rf.sort_rack_ids(rack_id, total_nracks)
  assert sorted_rack_id == [0, 1, 2, 3, 4, 5]


def test_sort_rack_id_4():
  rack_id = 5
  total_nracks = 6
  sorted_rack_id = rf.sort_rack_ids(rack_id, total_nracks)
  assert sorted_rack_id == [5, 4, 3, 2, 1, 0]