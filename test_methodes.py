import os
import random
import unittest
from main import HaTool


class TestFunctions(unittest.TestCase):
    ha = HaTool()

    def test_loading(self):
        try:
            HaTool()
        except Exception:
            self.fail("INIT Error")

    def test_login(self):
        self.assertTrue(self.ha.login_value(), "Login Error")

    def test_overviewCalculation(self):
        testTrips = []
        for _ in range(5):
            testTrips.append(random.randint(1, 500))
        ignoreList = list(map(int, os.getenv("ignoreList").split(" ")))

        for n in ignoreList:
            try:
                testTrips.remove(n)
            except ValueError:
                continue

        for i in testTrips:
            database = self.ha.get_overview_data_from_database(i)
            if database.equals(self.ha.create_overview_value(self.ha.get_raw_data_from_database(i))):
                self.assertTrue(True, "no issue for trip "+str(i))
            else:
                self.assertTrue(False, "difference in trip " + str(i))