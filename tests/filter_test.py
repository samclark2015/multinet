from multinet import filters
from random import randint


def get_fake_data():
    return {
        ("simple.sam", "intS"): randint(0, 100),
        ("simple.sam", "intS", "timestampNanoSeconds"): randint(1000, 5000),
        ("simple.sam", "intS", "timestampSeconds"): randint(0, 1000),
    }


class TestAnyChange:
    def test_new_data(self):
        filter_ = filters.AnyChange()
        data = get_fake_data()
        f_data = filter_(data, 1)
        assert f_data == data
        data2 = get_fake_data()
        f_data2 = filter_(data2, 1)
        assert f_data2 == data2

    def test_dupe_data(self):
        filter_ = filters.AnyChange()
        data = get_fake_data()
        f_data = filter_(data, 1)
        assert f_data == data
        f_data2 = filter_(data, 1)
        assert f_data2 == {}
