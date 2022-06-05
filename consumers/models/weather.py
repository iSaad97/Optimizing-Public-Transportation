"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        if "weather" in message.topic =='com.cta.weather':
            try:
                value = message.value()
                self.temperature = value.get("temperature")
                self.status = value.get("status")

            except Exception as e:
                logger.debug("bad weather data for processing? %s, %s", value, e)
