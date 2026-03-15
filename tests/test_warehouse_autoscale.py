"""Tests for the warehouse auto-scaling module."""

from src.warehouse_autoscale import _recommend_size, _size_rank, SIZE_RANKS


class TestRecommendSize:
    def test_small_catalog(self):
        assert _recommend_size(5) == "Small"

    def test_medium_catalog(self):
        assert _recommend_size(50) == "Medium"

    def test_large_catalog(self):
        assert _recommend_size(200) == "Large"

    def test_xlarge_catalog(self):
        assert _recommend_size(800) == "X-Large"

    def test_2xlarge_catalog(self):
        assert _recommend_size(2000) == "2X-Large"

    def test_boundary_10gb(self):
        assert _recommend_size(10) == "Medium"

    def test_boundary_100gb(self):
        assert _recommend_size(100) == "Large"

    def test_boundary_500gb(self):
        assert _recommend_size(500) == "X-Large"

    def test_boundary_1000gb(self):
        assert _recommend_size(1000) == "2X-Large"


class TestSizeRank:
    def test_known_sizes(self):
        assert _size_rank("2X-Small") == 1
        assert _size_rank("X-Small") == 2
        assert _size_rank("Small") == 3
        assert _size_rank("Medium") == 4
        assert _size_rank("Large") == 5
        assert _size_rank("X-Large") == 6
        assert _size_rank("2X-Large") == 7
        assert _size_rank("3X-Large") == 8
        assert _size_rank("4X-Large") == 9

    def test_unknown_size_defaults_to_3(self):
        assert _size_rank("Unknown") == 3

    def test_size_ordering(self):
        assert _size_rank("Small") < _size_rank("Medium")
        assert _size_rank("Medium") < _size_rank("Large")
        assert _size_rank("Large") < _size_rank("X-Large")

    def test_all_ranks_present(self):
        assert len(SIZE_RANKS) == 9
