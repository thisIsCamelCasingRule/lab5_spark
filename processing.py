from datetime import datetime

from data_generation import FEEDBACK_CATEGORY


class TaxiDataProcessor:
    
    def __init__(self, df):
        self.df = df
        self.drivers_rating = self.df.filter(lambda trip: trip['driver_rate'] > 0).map(self._trip_driver_rate_map).reduceByKey(self._driver_rate_reduce).map(self._driver_avg_map)
        self.clients_rating = self.df.map(self._trip_client_map).reduceByKey(self._client_reduce).map(self._client_map)
        self.client_feedback = self.df.map(self._rate).reduce(self._client_feedback_reduce)

    @staticmethod
    def _trip_driver_rate_map(trip):
        return trip['driver'], (trip['driver_rate'], 1)

    @staticmethod
    def _driver_rate_reduce(acc, n):
        return acc[0] + n[0], acc[1] + n[1]

    @staticmethod
    def _driver_avg_map(driver):
        avg_rating = round(driver[1][0] / driver[1][1], 2)
        return driver[0], avg_rating

    @staticmethod
    def _trip_client_map(trip):
        return trip['client'], (trip['client_rate'], 1)

    @staticmethod
    def _client_map(client):
        avg_rating = round(client[1][0] / client[1][1], 2)
        return client[0], avg_rating

    @staticmethod
    def _client_reduce(acc, n):
        return acc[0] + n[0], acc[1] + n[1]

    def top_drivers(self, n):
        return self.drivers_rating.takeOrdered(n, key=lambda driver: -driver[1])

  
    @staticmethod
    def _trip_driver_map(trip):
        return trip['driver'], (trip['driver_rate'], 1)

    @staticmethod
    def _rate(trip):
        if len(trip['driver_feedback']) != 0:
            return trip['driver_feedback']

    @staticmethod
    def _client_feedback_reduce(acc, n):
        if (acc == 0):
            return [0, 0, 0]
        return [acc[0] + n[0], acc[1] + n[1], acc[2] + n[2]]
