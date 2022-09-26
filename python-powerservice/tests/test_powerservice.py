""" Tests for the powerservice"""
import pytest
from powerservice.trading import check_if_valid_date, generate_new_random_trade_position

cases = [("", False),
         ("01/04/2015", True),
         ("30/04/2015", True),
         (1, False),
         ("01/30/2015", False),
         ("30/04/bla", False)
         ]


@pytest.mark.parametrize("date,expected", cases)
def test_date_checker(date, expected):
    """ Check that only d/m/y formatted date is accepted"""
    assert check_if_valid_date(date) == expected

#
# def test_generate_new_random_trade_position_period():
#     """ Try to generate a new random trade position"""
#     new_trade = generate_new_random_trade_position(date="01/04/2015")
#     period_list = new_trade["period"]
#
#     assert period_list[0] == 1 and period_list[-1] == 24

def test_generate_new_random_trade_position_time_series_len():
    """Check that the period and volume series are of the same length"""
    new_trade = generate_new_random_trade_position(date="01/04/2015")
    period_list = new_trade["time"]
    volume_list = new_trade["volume"]

    assert len(period_list) == len(volume_list)
